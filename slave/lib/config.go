package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
)

type SlaveConfig struct {
	Name     string `json:"Name"`
	DataDir  string `json:"DataDir"`
	Magic    uint32 `json:"Magic"`
	Compress bool   `json:"Compress"`
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
	Logger.Println("config loaded")
}
