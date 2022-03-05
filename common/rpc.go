package common

import (
	"encoding/json"
	"log"
	"os"
	"path"

	"github.com/fsnotify/fsnotify"
)

type RPCPair struct {
	MasterAddr string `json:"MasterAddr"`
	SlaveAddr  string `json:"SlaveAddr"`
}

type RPCPairManager struct {
	file    string
	pairs   []RPCPair
	watcher *fsnotify.Watcher
	events  chan struct{}
}

func (m *RPCPairManager) GetPairs() []RPCPair {
	return m.pairs
}

func (m *RPCPairManager) GetEventChan() <-chan struct{} {
	return m.events
}

func (m *RPCPairManager) Close() {
	m.watcher.Close()
	close(m.events)
}

func (m *RPCPairManager) reload() {
	configPath, err := os.UserConfigDir()
	if err != nil {
		log.Println(err)
		return
	}
	configPath = path.Join(configPath, "bltrader", "rpc.json")
	configContent, err := os.ReadFile(configPath)
	if err != nil {
		log.Println(err)
		return
	}
	var pairs []RPCPair
	err = json.Unmarshal(configContent, &pairs)
	if err != nil {
		log.Println(err)
		return
	}
	m.pairs = pairs
}

func (m *RPCPairManager) WatchForChange() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Write == fsnotify.Write {
					m.reload()
					m.events <- struct{}{}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()
	watcher.Add(m.file)
}

func CreateRPCPairManager() *RPCPairManager {
	file, err := os.UserConfigDir()
	if err != nil {
		log.Fatal(err)
	}
	file = path.Join(file, "bltrader", "rpc.json")
	configContent, err := os.ReadFile(file)
	if err != nil {
		log.Fatal(err)
	}
	var pairs []RPCPair
	err = json.Unmarshal(configContent, &pairs)
	if err != nil {
		log.Fatal(err)
	}

	pm := new(RPCPairManager)
	pm.file = file
	pm.pairs = pairs
	pm.events = make(chan struct{})

	return pm
}
