package lib

import (
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"

	"github.com/thezzisu/bltrader/common"
)

type RemoteSubscribeRequest struct {
	stock int32
	etag  int32
	ch    chan *common.BLOrder
}

type Remote struct {
	hub            *Hub
	manager        *common.RPCPairManager
	transports     []*Transport
	transportMutex sync.RWMutex
	name           string
	incoming       chan *common.BLOrderDTO
	subscribes     chan RemoteSubscribeRequest
	command        chan *common.BLTradeDTO
}

func CreateRemote(hub *Hub, name string) *Remote {
	configPath, _ := os.UserConfigDir()
	configPath = path.Join(configPath, "bltrader", fmt.Sprintf("rpc.%s.%s.json", name, Config.Name))

	r := new(Remote)
	r.hub = hub
	r.manager = common.CreateRPCPairManager(configPath)
	r.name = name
	r.incoming = make(chan *common.BLOrderDTO)
	r.subscribes = make(chan RemoteSubscribeRequest)
	r.command = make(chan *common.BLTradeDTO)
	return r
}

func (r *Remote) Reload() {
	r.transportMutex.Lock()
	defer r.transportMutex.Unlock()

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

func (r *Remote) Allocate(stock int32, etag int32, handshake int32) {
	r.transportMutex.RLock()
	defer r.transportMutex.RUnlock()
	if len(r.transports) == 0 {
		// Since we do not have any transports,
		// just do not allocate at all!
		return
	}
	bestK, bestV := 0, atomic.LoadInt32(&r.transports[0].subscriptionCount)
	for i := 1; i < len(r.transports); i++ {
		v := atomic.LoadInt32(&r.transports[i].subscriptionCount)
		if v > bestV {
			bestK, bestV = i, v
		}
	}
	Logger.Printf("Remote[%s].Allocate: stock %d from %d using %d\n", r.name, stock, etag, bestK)
	r.transports[bestK].Allocate(stock, etag, handshake)
}

func (r *Remote) RecvLoop() {
	pending := make(map[int32]RemoteSubscribeRequest)
	subscription := make(map[int32]chan *common.BLOrder)
	handshake := int32(0)
	for {
		select {
		case dto := <-r.incoming:
			if common.IsCmd(dto.Mix) {
				// Logger.Printf("Remote[%s].RecvLoop: got command\n", r.name)
				cmd, payload := common.DecodeCmd(dto.Mix)
				// Command
				switch cmd {
				case common.CmdSubReq:
					// Subscribe Request
					// Use payload as StkId, Price as etag, OrderId as handshake
					r.Allocate(payload, dto.Price, dto.OrderId)

				case common.CmdSubRes:
					// Subscribe Response
					// Use BidId as handshake
					if req, ok := pending[dto.OrderId]; ok {
						subscription[req.stock] = req.ch
						delete(pending, dto.OrderId)
					}
				}
			} else {
				// Data
				var order common.BLOrder
				common.UnmarshalOrderDTO(dto, &order)
				if ch, ok := subscription[order.StkCode]; ok {
					ch <- &order
				}
			}

		case req := <-r.subscribes:
			delete(subscription, req.stock)
			handshake++
			pending[handshake] = req
			r.command <- &common.BLTradeDTO{
				Mix:   common.EncodeCmd(common.CmdSubReq, req.stock),
				AskId: handshake,
				Price: req.etag,
			}
		}
	}
}

func (r *Remote) Start() {
	go r.MainLoop()
	go r.RecvLoop()
}

func (r *Remote) Subscribe(stock int32, etag int32) <-chan *common.BLOrder {
	Logger.Printf("Remote[%s].Subscribe: stock %d from %d\n", r.name, stock, etag)
	ch := make(chan *common.BLOrder)
	r.subscribes <- RemoteSubscribeRequest{stock: stock, etag: etag, ch: ch}
	return ch
}
