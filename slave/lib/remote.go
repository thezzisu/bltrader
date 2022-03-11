package lib

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thezzisu/bltrader/common"
)

type RemoteSubscribeRequest struct {
	stock  int32
	etag   int32
	result chan chan *common.BLOrder
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
	reshape        chan struct{}
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
	r.reshape = make(chan struct{})
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
		if v < bestV {
			bestK, bestV = i, v
		} else if v == bestV && rand.Int()&1 == 0 {
			bestK = i
		}
	}
	Logger.Printf("Remote[%s].Allocate: stock %d from %d using %d\n", r.name, stock, etag, bestK)
	r.transports[bestK].Allocate(stock, etag, handshake)
}

func (r *Remote) RecvLoop() {
	pending := make(map[int32]RemoteSubscribeRequest)
	pendingTimeout := make(chan int32)

	subscription := make(map[int32]chan *common.BLOrder)
	handshake := int32(0)

	timeout := time.Duration(Config.SubscribeTimeoutMs) * time.Millisecond
	for {
		select {
		case dto := <-r.incoming:
			if common.IsCmd(dto.Mix) {
				cmd, payload := common.DecodeCmd(dto.Mix)
				switch cmd {
				case common.CmdSubReq: // Subscribe Request, use payload as StkId, Price as etag, OrderId as handshake
					go r.Allocate(payload, dto.Price, dto.OrderId)

				case common.CmdSubRes: // Subscribe Response, use BidId as handshake
					if req, ok := pending[dto.OrderId]; ok {
						ch := make(chan *common.BLOrder)
						subscription[req.stock] = ch
						req.result <- ch
						delete(pending, dto.OrderId)
					}
				}
			} else {
				var order common.BLOrder
				common.UnmarshalOrderDTO(dto, &order)
				// 100ms data processing delay
				timer := time.NewTimer(time.Millisecond * 100)
				if ch, ok := subscription[order.StkCode]; ok {
					select {
					case ch <- &order:
					case <-timer.C:
						close(ch)
						delete(subscription, order.StkCode)
					}
				}
			}

		case hs := <-pendingTimeout:
			if req, ok := pending[hs]; ok {
				req.result <- nil
				delete(pending, hs)
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
			go func() {
				time.Sleep(timeout)
				pendingTimeout <- handshake
			}()
		}
	}
}

func (r *Remote) ShaperLoop() {
	interval := time.Millisecond * time.Duration(Config.ShaperIntervalMs)
	for {
		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
			Logger.Printf("Remote[%s].ShaperLoop: triggered by timer\n", r.name)
		case <-r.reshape:
			Logger.Printf("Remote[%s].ShaperLoop: triggered by transport\n", r.name)
		}
		r.transportMutex.RLock()

		min, max := int32(math.MaxInt32), int32(0)
		k := 0
		for i, t := range r.transports {
			count := atomic.LoadInt32(&t.subscriptionCount)
			if count < min {
				min = count
			}
			if count > max {
				max = count
				k = i
			}
		}
		if max >= 2 && max-min > 0 {
			Logger.Printf("Remote[%s].ShaperLoop: re-allocate %d", r.name, k)
			r.transports[k].ReAllocate()
		}
		r.transportMutex.RUnlock()
	}
}

func (r *Remote) Start() {
	go r.MainLoop()
	go r.RecvLoop()
	go r.ShaperLoop()
}

func (r *Remote) Subscribe(stock int32, etag int32) <-chan *common.BLOrder {
	Logger.Printf("Remote[%s].Subscribe: stock %d since %d\n", r.name, stock, etag)
	result := make(chan chan *common.BLOrder)
	r.subscribes <- RemoteSubscribeRequest{stock: stock, etag: etag, result: result}
	ch := <-result
	return ch
}
