package main
import "fmt"
import "os"
import "time"
import "math"
import "strconv"
import "github.com/go-redis/redis"

const {
    DB = []string{"accounts", "workers", "shares", "blocks"}
    host = "127.0.0.1"
    port = "6379"
    passwd = ""

    interval = 100
    ConstBlake2B = 4295032833000
    StepBlake2B = 180
}

func main() {
    if len(os.Args[1:]) != 1 {
        fmt.Printf("Usage:\n\t%s <wallet address>\n", os.Args[0])
        return errors.New("Error argument")
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
        _, err = conn.Ping().Result()
        if err == nil {
            i++
        }
    }

    if i != len(DB) {
        fmt.Println("Error connecting to redis.")
        return errors.New("Error connection to redis")
    }

    clientID, err := redisConn["accounts"].Get(os.Args[1]).Result()
    if err == redis.Nil {
        fmt.Println("wallet address not found")
        return errors.New("wallet address not found")
    }
    if err != nil {
        fmt.Println(err)
        return err
    }
    workerList, err := GetWorkers(clientID, redisConn["workers"])

    for workerID, workerName := range workerList {
        shareList, err := GetShares(clientID, workerID, redisConn["shares"])
        if err != nil {
            fmt.Println(err)
            return err
        }
        shareList = *shareList
        fmt.Printf(
            "%s:\t%s\treject:%d\n",
            workerName,
            FormatHashrate(shareList["hashrate"].(float64)),
            shareList["badshare"].(uint64)
        )
    }

}

func GetWorkers(clientID string, conn *redis.Client) *map[string]string, err {
    workerList := make(map[string]string)
    cursor := 0
    match := fmt.Sprintf("%s.*", clientID)
    count := 100
    for {
        keys, cursor, err := conn.Scan(cursor, match, count).Result()
        for key := range keys {
            s := strings.SplitN（key, ".", -1)
            //workerID := strconv.ParseUint(s[1], 10, 64)
            workerID := s[1]
            workerName, err := conn.HGet(key, "worker").Result()
            if err != nil {
                fmt.Println(err)
                return nil, err
            }
            workerList[workerID] = workerName
        }
        if cursor == 0 {
            break
        }
    }
    return &workerList, nil
}

func GetShares(clientID string, workerID string, conn *redis.Client) *map[string]interface{}, err {
    shareList := make(map[string]interface{})
    cursor := 0
    match := fmt.Sprintf("%s.%s.*", clientID, workerID)
    count := 100
    currentTime := time.Now().Unix()
    var Shares uint64
    var numShares uint64
    var invalidShares uint64
    var numInvalidShares uint64
    for {
        keys, cursor, err := conn.Scan(cursor, match, count).Result()
        for key := range keys {
            s := strings.SplitN(key, ".", -1)
            share_time := strconv.ParseInt(s[2], 10, 64)
            delay := currentTime - share_time
            if share_time <= delay {
                continue
            }

            validFlag, err := conn.HGet(key, "valid").Result()
            if err != nil {
                fmt.Println(err)
                return nil, err
            }
            shares, err := conn.HGet(key, "difficulty").Result()
            if err != nil {
                fmt.Println(err)
                return nil, err
            }
            validFlag = int(validFlag)
            if validFlag == 0 {
                numInvalidShares++
                invalidShares += shares
            }
            numShares++
            Shares += shares
        }
        if cursor == 0 {
            break
        }
    }
    shareList["hashrate"] = float64(Shares) * ConstBlake2B / StepBlake2B / 1000
    shareList["badrate"] = float64(numInvalidShares) * (Shares / numShares) * ConstBlake2B / StepBlake2B / 1000
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
