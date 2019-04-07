package main
import (
    "os"
    "fmt"
    "log"
    "time"
    "math"
    "strings"
    "strconv"
    "errors"

    "encoding/json"

    "io"
    "io/ioutil"
    "net/http"

    "github.com/go-redis/redis"
    "github.com/gorilla/mux"
)

var (
    hr *HashrateReader
)

type HashrateReader struct {
    redisConn   map[string]*redis.ClusterClient
    config      *Config
}

type ClientReader struct {
    w           io.Writer
    clientID    string

    hr          *HashrateReader
    redisConn   *map[string]*redis.ClusterClient
    workerList  *map[string]interface{}
}

type Config struct {
    Hosts   []string            `json:"hosts"`
    Ports   []uint              `json:"ports"`
    Tables  []string            `json:"tables"`
    Passwd  string              `json:"passwd"`
    Server  string              `json:"server"`
    ExpireTime      uint64          `json:"expire"`
    Blake2BConst    uint64          `json:"blake2b"`
    UpdateDelay     int64           `json:"delay"`
    Periods         []int64         `json:"periods"`
    ListenAddr      string          `json:"listenaddr"`
    Path            string          `json:"path"`
    Method          string          `json:"method"`
    Timeout         uint64          `json:"timeout"`
    MaxHeaderBytes  uint            `json:"maxheaderbytes"`
}

func main() {
    if len(os.Args[1:]) != 1 {
        fmt.Printf("usage:\n\t%s <config.json>\n", os.Args[0])
        return
    }
    config, err := ReadConfig(os.Args[1])
    if err != nil {
        log.Fatal(err)
        return
    }
    hr = &HashrateReader{}
    err = hr.SetupRedisConn(config)
    if err != nil {
        fmt.Println(err)
        return
    }
    defer hr.CloseRedisConn()
    r := mux.NewRouter()
    r.HandleFunc(config.Path, HashrateHandler).Methods(config.Method)
    srv := &http.Server{
        Handler:        r,
        Addr:           config.ListenAddr,
        ReadTimeout:    time.Duration(config.Timeout) * time.Second,
        WriteTimeout:   time.Duration(config.Timeout) * time.Second,
        MaxHeaderBytes: 1 << config.MaxHeaderBytes,
    }
    fmt.Println("Start listen on "+config.ListenAddr)
    log.Fatal(srv.ListenAndServe())
}

func ReadConfig(file string) (*Config, error) {
    config := Config{}
    res, err := ioutil.ReadFile(file)
    if err != nil {
        return nil, err
    }
    err = json.Unmarshal(res, &config)
    if err != nil {
        return nil, err
    }
    return &config, nil
}

func HashrateHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Server", hr.config.Server)
    cr := &ClientReader{}
    cr.Setup(hr, w)
    clientName := r.FormValue("wallet")

    finalJson := make(map[string]interface{})

    errcode := 0
    if len(clientName) == 0 {
        errcode = 1
        finalJson["error"] = errcode
        finalJson["data"] = "no wallet address"
        b, _ := json.Marshal(finalJson)
        w.Write(b)
        return
    }

    err := cr.SetupWallet(clientName)
    if err != nil {
        io.WriteString(w, fmt.Sprintln(err))
        return
    }

    data := make(map[string]interface{})

    s, err := cr.GetOnlineStatus()
    if err != nil {
        errcode--
    } else {
        data["online_status"] = s
    }

    for i, period := range hr.config.Periods {
       s, err = cr.GetHashrate(period)
       if err != nil {
           errcode--
       } else {
           data[fmt.Sprintf("hashrate%d", i+1)] = s
       }
    }

    finalJson["error"] = errcode
    finalJson["data"] = data

    b, err := json.Marshal(finalJson)
    w.Write(b)
}

func (hr *HashrateReader) SetupRedisConn(config *Config) error {
    hr.config = config
    redisConn := make(map[string]*redis.ClusterClient)
    for i := 0; i < len(hr.config.Tables); i++ {
        hosts := []string{}
        for _, host := range config.Hosts {
            hosts = append(hosts, fmt.Sprintf("%s:%d", host, config.Ports[i]))
        }
        redisConn[config.Tables[i]] = redis.NewClusterClient(&redis.ClusterOptions{
            Addrs:      hosts,
            Password:   config.Passwd,
        })
    }
    i := 0
    for _, conn := range redisConn {
        _, err := conn.Ping().Result()
        if err == nil {
            i++
        }
    }

    if i != len(hr.config.Tables) {
        fmt.Println("Error connecting to redis.")
        return errors.New("Error connecting to redis.")
    }

    hr.redisConn = redisConn
    return nil
}

func (hr *HashrateReader) CloseRedisConn() {
    for _, conn := range hr.redisConn {
        conn.Close()
    }
}

func (hr *HashrateReader) GetRedisConn() *map[string]*redis.ClusterClient {
    return &hr.redisConn
}

func (cr *ClientReader) Setup(hr *HashrateReader, writer io.Writer) {
    cr.redisConn = hr.GetRedisConn()
    cr.hr = hr
    cr.w = writer
}

func (cr *ClientReader) SetupWallet(wallet string) error {
    clientID, err := (*cr.redisConn)["accounts"].Get(wallet).Result()
    if err == redis.Nil {
        io.WriteString(cr.w, fmt.Sprintln("wallet address not found"))
        return errors.New("Wallet address not found")
    }
    if err != nil {
        io.WriteString(cr.w, fmt.Sprintln(err))
        return err
    }

    cr.clientID = clientID
    return nil
}

func (cr *ClientReader) GetOnlineStatus() (*map[string]interface{}, error) {
    if cr.workerList == nil {
        err := cr.GetWorkers()
        if err != nil {
            return nil, err
        }
    }
    onlineStatusReport := make(map[string]string)
    var tAgo time.Duration
    for _, workerRecord := range *cr.workerList {
        ttl := workerRecord.(map[string]interface{})["ttl"].(time.Duration)
        workerName := workerRecord.(map[string]interface{})["name"].(string)
        tAgo = time.Duration(cr.hr.config.ExpireTime) * time.Second - ttl
        statusStr := fmt.Sprintf(
            "%.0fHour%.0fMin%.0fSec Ago",
            math.Floor(tAgo.Hours()),
            math.Floor(tAgo.Minutes() - 60 * math.Floor(tAgo.Hours())),
            math.Floor(tAgo.Seconds() - 60 * math.Floor(tAgo.Minutes())))
        if ttl < 0 {
            onlineStatusReport[workerName] = "online"
        } else { //ttl >= 0, worker offline
            //key already setup
            if key, exist := onlineStatusReport[workerName]; exist {
                if strings.Compare(key, "online") == 0 {
                    continue
                }
                if strings.Compare(statusStr, key) < 0 {
                    onlineStatusReport[workerName] = statusStr
                }
            } else {//key not setup yet
                onlineStatusReport[workerName] = statusStr
            }
        }
    }
    jsonObject := make(map[string]interface{})
    for workerName, status := range onlineStatusReport {
        jsonObject[workerName] = status
    }
    return &jsonObject, nil
}

func (cr *ClientReader) GetHashrate(interval int64) (*map[string]interface{}, error) {
    if cr.workerList == nil {
        err := cr.GetWorkers()
        if err != nil {
            io.WriteString(cr.w, fmt.Sprintln(err))
            return nil, err
        }
    }
    hashrateReport := make(map[string]interface{})
    for workerID, workerRecord := range *cr.workerList {
        shareList, err := cr.GetShares(workerID, interval)
        if err != nil {
            io.WriteString(cr.w, fmt.Sprintln(err))
            return nil, err
        }
        shares := *shareList
        workerName := workerRecord.(map[string]interface{})["name"].(string)
        if oldRecord, exist := hashrateReport[workerName]; exist {
            hashrateRecord := make(map[string]interface{})
            hashrateRecord["id"] = append(oldRecord.(map[string]interface{})["id"].([]string), workerID)
            hashrateRecord["hashrate"] = oldRecord.(map[string]interface{})["hashrate"].(float64) + shares["hashrate"].(float64)
            hashrateRecord["shares"] = oldRecord.(map[string]interface{})["shares"].(uint64) + shares["shares"].(uint64)
            hashrateRecord["badshare"] = oldRecord.(map[string]interface{})["badshare"].(uint64) + shares["badshare"].(uint64)
            hashrateReport[workerName] = hashrateRecord
        } else {
            hashrateRecord := make(map[string]interface{})
            hashrateRecord["id"] = []string{workerID}
            hashrateRecord["hashrate"] = shares["hashrate"].(float64)
            hashrateRecord["shares"] = shares["shares"].(uint64)
            hashrateRecord["badshare"] = shares["badshare"].(uint64)
            hashrateReport[workerName] = hashrateRecord
        }
    }
    jsonObject := make(map[string]interface{})
    for workerName, hashRecord := range hashrateReport {
        jsonObject[workerName] = []interface{}{
            cr.FormatHashrate(hashRecord.(map[string]interface{})["hashrate"].(float64)),
            hashRecord.(map[string]interface{})["hashrate"].(float64),
            hashRecord.(map[string]interface{})["shares"].(uint64),
            hashRecord.(map[string]interface{})["badshare"].(uint64),
        }
    }
    return &jsonObject, nil
}

func (cr *ClientReader) GetWorkers() error {
    if len(cr.clientID) == 0 {
        io.WriteString(cr.w, fmt.Sprintln("empty clientID"))
        return errors.New("empty clientID")
    }
    conn := (*cr.redisConn)["workers"]
    workerList := make(map[string]interface{})
    var cursor uint64
    match := fmt.Sprintf("%s.*", cr.clientID)
    count := int64(10)
    for {
        var keys []string
        var err error
        keys, cursor, err = conn.Scan(cursor, match, count).Result()
        if err != nil {
            io.WriteString(cr.w, fmt.Sprintln(err))
            return err
        }
        for _, key := range keys {
            s := strings.SplitN(key, ".", -1)
            workerID := s[1]
            workerName, err := conn.HGet(key, "worker").Result()
            if err != nil {
                io.WriteString(cr.w, fmt.Sprintln(err))
                return err
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
    cr.workerList = &workerList
    return nil
}

func (cr *ClientReader) GetShares(workerID string, interval int64) (*map[string]interface{}, error) {
    conn := (*cr.redisConn)["shares"]
    shareList := make(map[string]interface{})
    cursor := uint64(0)
    match := fmt.Sprintf("%s.%s.*", cr.clientID, workerID)
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
            share_time, err := strconv.ParseInt(s[3], 10, 64)
            if err != nil {
                fmt.Println(err)
                return nil, err
            }
            startTime := currentTime - interval - cr.hr.config.UpdateDelay
            endTime := currentTime - cr.hr.config.UpdateDelay
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
    hashrate = Shares * float64(cr.hr.config.Blake2BConst) / float64(interval) / 1000
    shareList["hashrate"] = hashrate
    shareList["shares"] = numShares
    shareList["badshare"] = numInvalidShares

    return &shareList, nil
}

func (cr *ClientReader) FormatHashrate(h float64) string {
    var result string
    if h >= math.Pow(1000, 5) {
        result = fmt.Sprintf("%.3fP", h / math.Pow(1000, 5))
    } else if h >= math.Pow(1000, 4) {
        result = fmt.Sprintf("%.3fT", h / math.Pow(1000, 4))
    } else if h >= math.Pow(1000, 3) {
        result = fmt.Sprintf("%.3fG", h / math.Pow(1000, 3))
    } else if h >= math.Pow(1000, 2) {
        result = fmt.Sprintf("%.3fM", h / math.Pow(1000, 2))
    } else if h >= 1000 {
        result = fmt.Sprintf("%.3fK", h / 1000)
    } else {
        result = fmt.Sprintf("%.3f", h)
    }
    return result
}
