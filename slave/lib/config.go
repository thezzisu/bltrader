package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
)

type MasterInfo struct {
	Name string `json:"Name"`
}

type SlaveConfig struct {
	Name     string       `json:"Name"`
	DataDir  string       `json:"DataDir"`
	Magic    uint32       `json:"Magic"`
	Compress bool         `json:"Compress"`
	Procs    int          `json:"Procs"`
	Masters  []MasterInfo `json:"Masters"`
	Stocks   []int32      `json:"Stocks"`
}

var Config SlaveConfig

func init() {
	configPath, err := os.UserConfigDir()
	if err != nil {
		Logger.Fatal(err)
	}
	configPath = path.Join(configPath, "bltrader", "slave.json")
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
