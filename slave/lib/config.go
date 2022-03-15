package lib

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
)

type MasterInfo struct {
	Name string `json:"Name"`
}

type SlaveConfig struct {
	Name                  string       `json:"Name"`
	DataDir               string       `json:"DataDir"`
	Magic                 uint32       `json:"Magic"`
	Procs                 int          `json:"Procs"`
	Masters               []MasterInfo `json:"Masters"`
	Stocks                []int32      `json:"Stocks"`
	StockHandlerTimeoutMs int          `json:"StockHandlerTimeoutMs"`
	ShaperIntervalMs      int          `json:"ShaperIntervalMs"`
	SubscribeTimeoutMs    int          `json:"SubscribeTimeoutMs"`
	ProcessTimeoutMs      int          `json:"ProcessTimeoutMs"`
	FlushIntervalMs       int          `json:"FlushIntervalMs"`
	SendBufferSize        int          `json:"SendBufferSize"`
}

var Config SlaveConfig
var Profile string

func init() {
	flag.StringVar(&Profile, "profile", "slave", "profile name")
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
	var config SlaveConfig
	err = json.Unmarshal(configContent, &config)
	if err != nil {
		Logger.Fatal(err)
	}
	Config = config
	Logger.SetPrefix(fmt.Sprintf("[slave %s] ", Config.Name))
	stockMap := make(map[int32]struct{})
	for _, stock := range Config.Stocks {
		if _, ok := stockMap[stock]; ok {
			Logger.Fatalln("duplicate stock:", stock)
		}
		stockMap[stock] = struct{}{}
	}
	Logger.Println("config loaded")
}
