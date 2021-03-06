package lib

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
)

type SlaveInfo struct {
	Name   string  `json:"Name"`
	Stocks []int32 `json:"Stocks"`
}

type MasterConfig struct {
	Name               string      `json:"Name"`
	CacheDir           string      `json:"CacheDir"`
	DataDir            string      `json:"DataDir"`
	Magic              uint32      `json:"Magic"`
	Procs              int         `json:"Procs"`
	Slaves             []SlaveInfo `json:"Slaves"`
	StockRecvTimeoutMs int         `json:"StockRecvTimeoutMs"`
	ShaperIntervalMs   int         `json:"ShaperIntervalMs"`
	SubscribeTimeoutMs int         `json:"SubscribeTimeoutMs"`
	ProcessTimeoutMs   int         `json:"ProcessTimeoutMs"`
	FlushIntervalMs    int         `json:"FlushIntervalMs"`
	SendBufferSize     int         `json:"SendBufferSize"`
}

var Config MasterConfig
var StockMap map[int32]string
var SlaveMap map[string][]int32
var Profile string

func init() {
	flag.StringVar(&Profile, "profile", "master", "profile name")
}

func InitConfig() {
	configPath, err := os.UserConfigDir()
	if err != nil {
		Logger.Fatal(err)
	}
	configPath = path.Join(configPath, "bltrader", fmt.Sprintf("%s.json", Profile))
	Logger.Println("load config from", configPath)
	configContent, err := os.ReadFile(configPath)
	if err != nil {
		Logger.Fatal(err)
	}
	var config MasterConfig
	err = json.Unmarshal(configContent, &config)
	if err != nil {
		Logger.Fatal(err)
	}
	Config = config
	Logger.SetPrefix(fmt.Sprintf("[master %s] ", Config.Name))
	StockMap = make(map[int32]string)
	SlaveMap = make(map[string][]int32)
	for _, slave := range Config.Slaves {
		if _, ok := SlaveMap[slave.Name]; ok {
			Logger.Fatalln("duplicate slave name:", slave.Name)
		}
		SlaveMap[slave.Name] = slave.Stocks
		for _, stock := range slave.Stocks {
			if _, ok := StockMap[stock]; ok {
				Logger.Fatalln("duplicate stock:", stock)
			}
			StockMap[stock] = slave.Name
		}
	}
	Logger.Println("config loaded")
}
