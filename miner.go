package main
import "fmt"
import "os"
import "time"
import "math"
import "strings"
import "strconv"
import "github.com/go-redis/redis"

const (
    host = "127.0.0.1"
    port = "6379"
    passwd = ""

    ConstBlake2B = 4295032833000
    UpdateDelay = 35
    Interval = 300
)

var (
    DB = []string{"accounts", "workers", "shares", "blocks"}
    redisExpireTime = 48 * 60 * 60 * time.Second
)

func main() {
    if len(os.Args[1:]) != 1 {
        fmt.Printf("Usage:\n\t%s <wallet address>\n", os.Args[0])
        return
    }
    redisConn := make(map[string]*redis.Client)
    for i := 0; i < 4; i++ {
        redisConn[DB[i]] = redis.NewClient(&redis.Options{
            Addr:       fmt.Sprintf("%s:%s", host, port),
            Password:   passwd,
            DB:         i,
        })
    }
    i := 0
    for _, conn := range redisConn {
        _, err := conn.Ping().Result()
        if err == nil {
            i++
        }
    }

    if i != len(DB) {
        fmt.Println("Error connecting to redis.")
        return
    }

    clientID, err := redisConn["accounts"].Get(os.Args[1]).Result()
    if err == redis.Nil {
        fmt.Println("wallet address not found")
        return
    }
    if err != nil {
        fmt.Println(err)
        return
    }
    workerList, err := GetWorkers(clientID, redisConn["workers"])
    workers := *workerList

    for _, workerRecord := range workers {
        ttl := workerRecord.(map[string]interface{})["ttl"].(time.Duration)
        if ttl < 0 {
            fmt.Printf("worker: %s online\n", workerRecord.(map[string]interface{})["name"].(string))
        } else {
            tAgo := redisExpireTime - ttl
            fmt.Printf("worker: %s %.0fHour%.0fMin%.0fSec Ago\n", workerRecord.(map[string]interface{})["name"].(string), math.Floor(tAgo.Hours()), math.Floor(tAgo.Minutes() - 60 * math.Floor(tAgo.Hours())), math.Floor(tAgo.Seconds() - 60 * math.Floor(tAgo.Minutes())))
        }

    }

    fmt.Println("5 minutes:")
    PrintHashrate(clientID, workerList, 300, redisConn["shares"])

    fmt.Println("15 minutes:")
    PrintHashrate(clientID, workerList, 900, redisConn["shares"])

    fmt.Println("30 minutes:")
    PrintHashrate(clientID, workerList, 1800, redisConn["shares"])

    fmt.Println("1 hour:")
    PrintHashrate(clientID, workerList, 3600, redisConn["shares"])
}

func PrintHashrate(clientID string, workerList *map[string]interface{}, interval int64, conn *redis.Client) {
    hashrateReport := make(map[string]interface{})
    for workerID, workerRecord := range *workerList {
        shareList, err := GetShares(clientID, workerID, interval, conn)
        if err != nil {
            fmt.Println(err)
            return
        }
        shares := *shareList
        workerName := workerRecord.(map[string]interface{})["name"].(string)
        if oldRecord, exist := hashrateReport[workerName]; exist {
            hashrateRecord := make(map[string]interface{})
            hashrateRecord["id"] = append(oldRecord.(map[string]interface{})["id"].([]string), workerID)
            hashrateRecord["hashrate"] = oldRecord.(map[string]interface{})["hashrate"].(float64) + shares["hashrate"].(float64)
            hashrateRecord["badshare"] = oldRecord.(map[string]interface{})["badshare"].(uint64) + shares["badshare"].(uint64)
            hashrateReport[workerName] = hashrateRecord
        } else {
            hashrateRecord := make(map[string]interface{})
            hashrateRecord["id"] = []string{workerID}
            hashrateRecord["hashrate"] = shares["hashrate"].(float64)
            hashrateRecord["badshare"] = shares["badshare"].(uint64)
            hashrateReport[workerName] = hashrateRecord
        }
    }
    for workerName, hashRecord := range hashrateReport {
    fmt.Printf(
        "%s:\t%s\treject:%d\tid: %s\n",
        workerName,
        FormatHashrate(hashRecord.(map[string]interface{})["hashrate"].(float64)),
        hashRecord.(map[string]interface{})["badshare"].(uint64),
        hashRecord.(map[string]interface{})["id"].([]string))

    }
}

func GetWorkers(clientID string, conn *redis.Client) (*map[string]interface{}, error) {
    workerList := make(map[string]interface{})
    var cursor uint64
    match := fmt.Sprintf("%s.*", clientID)
    count := int64(10)
    for {
        var keys []string
        var err error
        keys, cursor, err = conn.Scan(cursor, match, count).Result()
        if err != nil {
            fmt.Println(err)
            return nil, err
        }
        for _, key := range keys {
            s := strings.SplitN(key, ".", -1)
            workerID := s[1]
            workerName, err := conn.HGet(key, "worker").Result()
            if err != nil {
                fmt.Println(err)
                return nil, err
            }
            workerRecord := make(map[string]interface{})
            workerRecord["name"] = workerName
            workerRecord["ttl"] = conn.TTL(key).Val()
            workerList[workerID] = workerRecord
        }
        if cursor == 0 {
            break
        }
    }
    return &workerList, nil
}

func GetShares(clientID string, workerID string, interval int64, conn *redis.Client) (*map[string]interface{}, error) {
    shareList := make(map[string]interface{})
    cursor := uint64(0)
    match := fmt.Sprintf("%s.%s.*", clientID, workerID)
    count := int64(1000)
    currentTime := time.Now().Unix()
    var Shares float64
    var numShares uint64
    var invalidShares float64
    var numInvalidShares uint64
    for {
        var keys []string
        var err error
        keys, cursor, err = conn.Scan(cursor, match, count).Result()
        if err != nil {
            fmt.Println(err)
            return nil, err
        }
        for _, key := range keys {
            s := strings.SplitN(key, ".", -1)
            share_time, err := strconv.ParseInt(s[2], 10, 64)
            if err != nil {
                fmt.Println(err)
                return nil, err
            }
            startTime := currentTime - interval - UpdateDelay
            endTime := currentTime - UpdateDelay
            if share_time < startTime || share_time > endTime {
                continue
            }

            validFlag, err := conn.HGet(key, "valid").Result()
            if err != nil {
                fmt.Println(err)
                return nil, err
            }
            sharesStr, err := conn.HGet(key, "difficulty").Result()
            if err != nil {
                fmt.Println(err)
                return nil, err
            }
            shares, err := strconv.ParseUint(sharesStr, 10, 64)
            if err != nil {
                fmt.Println(err)
                return nil, err
            }
            flag, err := strconv.ParseInt(validFlag, 10, 64)
            if flag == 0 {
                numInvalidShares++
                invalidShares += float64(shares)
            }
            numShares++
            Shares += float64(shares)
        }
        if cursor == 0 {
            break
        }
    }
    //var hashrate, badrate float64
    var hashrate float64
    hashrate = Shares * ConstBlake2B / float64(interval) / 1000
    //badrate = float64(numInvalidShares) * (Shares / float64(numShares)) * ConstBlake2B / float64(interval) / 1000
    shareList["hashrate"] = hashrate
    //shareList["badrate"] = badrate
    shareList["badshare"] = numInvalidShares

    return &shareList, nil
}

func FormatHashrate(h float64) string {
    var result string
    if h >= math.Pow(1000, 5) {
        result = fmt.Sprintf("%.2fP", h / math.Pow(1000, 5))
    } else if h >= math.Pow(1000, 4) {
        result = fmt.Sprintf("%.2fT", h / math.Pow(1000, 4))
    } else if h >= math.Pow(1000, 3) {
        result = fmt.Sprintf("%.2fG", h / math.Pow(1000, 3))
    } else if h >= math.Pow(1000, 2) {
        result = fmt.Sprintf("%.2fM", h / math.Pow(1000, 2))
    } else if h >= 1000 {
        result = fmt.Sprintf("%.2fK", h / 1000)
    } else {
        result = fmt.Sprintf("%.2f", h)
    }
    return result
}
