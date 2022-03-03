package slave

import (
	"encoding/json"
	"os"
	"path"
)

type SlaveConfig struct {
	Name string `json:"Name"`
}

func LoadSlaveConfig() SlaveConfig {
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
	return config
}
