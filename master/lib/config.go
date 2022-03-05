package lib

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
)

type MasterConfig struct {
	Name     string `json:"Name"`
	CacheDir string `json:"CacheDir"`
	DataDir  string `json:"DataDir"`
	Listen   string `json:"Listen"`
	Magic    uint32 `json:"Magic"`
	Compress bool   `json:"Compress"`
}

var Config MasterConfig

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
	Logger.Println("config loaded")
}
