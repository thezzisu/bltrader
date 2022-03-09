package lib

import (
	"fmt"
	"os"
	"path"

	"github.com/thezzisu/bltrader/common"
)

type RemoteSubscribeRequest struct {
	stock int32
	ch    chan *common.BLTrade
}

type Remote struct {
	hub        *Hub
	manager    *common.RPCPairManager
	transports []*Transport
	name       string
	incoming   chan *common.BLTradeDTO
	subscribes chan RemoteSubscribeRequest
	command    chan int32
}

func CreateRemote(hub *Hub, name string) *Remote {
	configPath, _ := os.UserConfigDir()
	configPath = path.Join(configPath, "bltrader", fmt.Sprintf("rpc.%s.%s.json", Config.Name, name))

	r := new(Remote)
	r.hub = hub
	r.manager = common.CreateRPCPairManager(configPath)
	r.name = name
	r.incoming = make(chan *common.BLTradeDTO)
	r.subscribes = make(chan RemoteSubscribeRequest)
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

func (r *Remote) Allocate(stock int32, etag int32) {
	// TODO
}

func (r *Remote) RecvLoop() {
	pending := make(map[int32]RemoteSubscribeRequest)
	subscription := make(map[int32]chan *common.BLTrade)
	id := int32(0)
	for {
		select {
		case dto := <-r.incoming:
			if dto.Mix == -1 {
				// Command
				// Use AskId as command type
				switch dto.AskId {
				case 0:
					// Subscribe Response
					// Use BidId as id
					if req, ok := pending[dto.BidId]; ok {
						subscription[req.stock] = req.ch
						delete(pending, dto.BidId)
					}

				case 1:
					// Subscribe Request
					// Use BidId as StkCode, Price as etag
					r.Allocate(dto.BidId, dto.Price)
				}
			} else {
				// Data
				var trade common.BLTrade
				common.UnmarshalTradeDTO(dto, &trade)
				if ch, ok := subscription[trade.StkCode]; ok {
					ch <- &trade
				}
			}

		case req := <-r.subscribes:
			delete(subscription, req.stock)
			id++
			pending[id] = req
			// TODO send subscribe request
		}
	}
}

func (r *Remote) Start() {
	go r.MainLoop()
}

func (r *Remote) Subscribe(stock int32, etag int32) <-chan *common.BLTrade {
	ch := make(chan *common.BLTrade)
	r.subscribes <- RemoteSubscribeRequest{stock: stock, ch: ch}
	return ch
}
