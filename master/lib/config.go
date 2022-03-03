package lib

import (
	"encoding/json"
	"os"
	"path"
)

type MasterConfig struct {
	Name string `json:"Name"`
}

func LoadMasterConfig() MasterConfig {
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
	return config
}
