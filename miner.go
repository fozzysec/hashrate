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
    errNoWorker = errors.New("No worker")
    errNoWallet = errors.New("No wallet address")
    errWalletNotFound = errors.New("Wallet address not found")
)

type HashrateReader struct {
    redisConn   map[string]*redis.ClusterClient
    config      *Config
}

const (
    ErrcodeNoWorker = 1
    ErrcodeNoWallet = 2
    ErrcodeWalletNotFound = 3
    ErrcodeOthers = -1
)

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

type ShareRecord struct {
    Shares              float64
    numShares           uint64
    InvalidShares       float64
    numInvalidShares    uint64
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
        errcode = ErrcodeNoWallet
    }

    err := cr.SetupWallet(clientName)
    if err != nil {
        if err == errWalletNotFound {
            errcode = ErrcodeWalletNotFound
        } else {
            errcode = ErrcodeOthers
        }
    }

    data := make(map[string]interface{})

    s, err := cr.GetOnlineStatus()
    if err != nil {
        if err == errNoWorker {
            errcode = ErrcodeNoWorker
        } else {
            errcode = ErrcodeOthers
        }
    } else {
        data["online_status"] = s
    }

    s, err = cr.GetHashrate(cr.hr.config.Periods)
    if err != nil {
        errcode = ErrcodeOthers
    } else {
        data["hashrate"] = s
    }

    if errcode == 0 {
        finalJson["data"] = data
    }

    finalJson["error"] = errcode
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
        return errWalletNotFound
    }
    if err != nil {
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

    if cr.workerList == nil {
        return nil, errNoWorker
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

func (cr *ClientReader) GetHashrate(periods []int64) (*map[string]interface{}, error) {
    if cr.workerList == nil {
        err := cr.GetWorkers()
        if err != nil {
            io.WriteString(cr.w, fmt.Sprintln(err))
            return nil, err
        }
    }
    hashrateReport := make(map[string]interface{})
    for workerID, workerRecord := range *cr.workerList {
        shareList, err := cr.GetShares(workerID, periods)
        //fmt.Printf("GetShares for worker %s done, %s\n", workerID, *shareList)
        if err != nil {
            io.WriteString(cr.w, fmt.Sprintln(err))
            return nil, err
        }
        shares := *shareList
        workerName := workerRecord.(map[string]interface{})["name"].(string)
        var CurrentWorkerRecord map[int64]interface{}
        if _, exist := hashrateReport[workerName]; exist {
            CurrentWorkerRecord = hashrateReport[workerName].(map[int64]interface{})
        } else {
            CurrentWorkerRecord = make(map[int64]interface{})
        }
        for interval, ShareData := range shares {
            //fmt.Println(ShareData)
            hashrateRecord := make(map[string]interface{})
            shareObj := ShareData.(map[string]interface{})
            if oldRecordInterface, exist := CurrentWorkerRecord[interval]; exist {
                oldRecord := oldRecordInterface.(map[string]interface{})
                //hashrateRecord["id"] = append(oldRecord["id"].([]string), workerID)
                hashrateRecord["hashrate"] = oldRecord["hashrate"].(float64) + shareObj["hashrate"].(float64)
                hashrateRecord["shares"] = oldRecord["shares"].(uint64) + shareObj["shares"].(uint64)
                hashrateRecord["badshare"] = oldRecord["badshare"].(uint64) + shareObj["badshare"].(uint64)
                CurrentWorkerRecord[interval] = hashrateRecord
            } else {
                //hashrateRecord["id"] = []string{workerID}
                hashrateRecord["hashrate"] = shareObj["hashrate"].(float64)
                hashrateRecord["shares"] = shareObj["shares"].(uint64)
                hashrateRecord["badshare"] = shareObj["badshare"].(uint64)
                CurrentWorkerRecord[interval] = hashrateRecord
            }
        }
        hashrateReport[workerName] = CurrentWorkerRecord
    }

    for workerName, hashrateRecords := range hashrateReport {
        for interval, hashrateRecord := range hashrateRecords.(map[int64]interface{}) {
            singleRecord := hashrateRecord.(map[string]interface{})
            hashrateReport[workerName].(map[int64]interface{})[interval].(map[string]interface{})["readable"] = FormatHashrate(singleRecord["hashrate"].(float64))
        }
    }
    return &hashrateReport, nil
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

func (cr *ClientReader) GetShares(workerID string, periods []int64) (*map[int64]interface{}, error) {
    conn := (*cr.redisConn)["shares"]
    shareList := make(map[int64]interface{})
    cursor := uint64(0)
    match := fmt.Sprintf("%s.%s.*", cr.clientID, workerID)
    count := int64(1000)
    currentTime := time.Now().Unix()
    ShareData := make([]ShareRecord, len(periods), len(periods))
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
            for i, interval := range periods {
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
                    ShareData[i].numInvalidShares++
                    ShareData[i].InvalidShares += float64(shares)
                }
                ShareData[i].numShares++
                ShareData[i].Shares += float64(shares)
            }
        }
        if cursor == 0 {
            break
        }
    }

    for i, interval := range periods {
        var hashrate float64
        currentRecord := make(map[string]interface{})
        hashrate = ShareData[i].Shares * float64(cr.hr.config.Blake2BConst) / float64(interval) / 1000
        currentRecord["hashrate"] = hashrate
        currentRecord["badrate"] = ShareData[i].InvalidShares
        currentRecord["shares"] = ShareData[i].numShares
        currentRecord["badshare"] = ShareData[i].numInvalidShares
        shareList[interval] = currentRecord
    }
    return &shareList, nil
}

func FormatHashrate(h float64) string {
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
