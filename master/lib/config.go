package lib

import (
	"encoding/json"
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
	Listen             string      `json:"Listen"`
	Magic              uint32      `json:"Magic"`
	Compress           bool        `json:"Compress"`
	Procs              int         `json:"Procs"`
	Slaves             []SlaveInfo `json:"Slaves"`
	StockRecvTimeoutMs int         `json:"StockRecvTimeoutMs"`
	ShaperIntervalMs   int         `json:"ShaperIntervalMs"`
	SubscribeTimeoutMs int         `json:"SubscribeTimeoutMs"`
}

var Config MasterConfig
var StockMap map[int32]string
var SlaveMap map[string][]int32

func init() {
	configPath, err := os.UserConfigDir()
	if err != nil {
		Logger.Fatal(err)
	}
	configPath = path.Join(configPath, "bltrader", "master.json")
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
