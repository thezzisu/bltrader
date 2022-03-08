package lib

import (
	"fmt"
	"os"
	"path"

	"github.com/thezzisu/bltrader/common"
)

type Remote struct {
	hub        *Hub
	manager    *common.RPCPairManager
	transports []*Transport

	name     string
	incoming chan common.BLTradeDTO
}

func CreateRemote(hub *Hub, name string) *Remote {
	configPath, _ := os.UserConfigDir()
	configPath = path.Join(configPath, "bltrader", fmt.Sprintf("rpc.%s.%s.json", Config.Name, name))

	r := new(Remote)
	r.hub = hub
	r.manager = common.CreateRPCPairManager(configPath)
	r.name = name
	r.incoming = make(chan common.BLTradeDTO)
	return r
}

func (r *Remote) Reload() {
	pairs := r.manager.GetPairs()

	n := len(r.transports)
	m := len(pairs)
	if n > m {
		for i := len(pairs); i < n; i++ {
			Logger.Printf("Remote[%s].Reload: closing endpoint %s <-> %s", r.name, r.transports[i].pair.MasterAddr, r.transports[i].pair.SlaveAddr)
			r.transports[i].Close()
		}
		r.transports = r.transports[:m]
		n = m
	}
	for i := 0; i < n; i++ {
		if pairs[i].MasterAddr != r.transports[i].pair.MasterAddr || pairs[i].SlaveAddr != r.transports[i].pair.SlaveAddr {
			Logger.Printf("Remote[%s].Reload: closing endpoint %s <-> %s", r.name, r.transports[i].pair.MasterAddr, r.transports[i].pair.SlaveAddr)
			r.transports[i].Close()
			r.transports[i] = CreateTransport(r, pairs[i])
			r.transports[i].Start()
			Logger.Printf("RPC[%s].Reload: new endpoint %s <-> %s", r.name, r.transports[i].pair.MasterAddr, r.transports[i].pair.SlaveAddr)
		}
	}
	for i := len(r.transports); i < len(pairs); i++ {
		endpoint := CreateTransport(r, pairs[i])
		r.transports = append(r.transports, endpoint)
		endpoint.Start()
		Logger.Printf("RPC[%s].Reload: new endpoint %s <-> %s", r.name, endpoint.pair.MasterAddr, endpoint.pair.SlaveAddr)
	}
}

func (r *Remote) MainLoop() {
	r.Reload()
	r.manager.WatchForChange()
	reload := r.manager.GetEventChan()
	for {
		<-reload
		r.Reload()
	}
}

func (r *Remote) RecvLoop() {
	for {
		dto := <-r.incoming
		if dto.Mix == -1 {
			// Command
		} else {
			// Data
			var trade common.BLTrade
			common.UnmarshalTradeDTO(&dto, &trade)
		}
	}
}

func (r *Remote) Start() {
	go r.MainLoop()
}

func (r *Remote) Subscribe(stock int32, etag int32) (<-chan common.BLTrade, bool) {
	ch := make(chan common.BLTrade)
	// TODO
	return ch, true
}
