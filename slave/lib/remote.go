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
			Logger.Printf("Remote\tReload %s: closing endpoint %s <-> %s", r.name, r.transports[i].pair.MasterAddr, r.transports[i].pair.SlaveAddr)
			r.transports[i].Close()
		}
		r.transports = r.transports[:m]
		n = m
	}
	for i := 0; i < n; i++ {
		if pairs[i].MasterAddr != r.transports[i].pair.MasterAddr || pairs[i].SlaveAddr != r.transports[i].pair.SlaveAddr {
			Logger.Printf("Remote\tReload %s: closing endpoint %s <-> %s", r.name, r.transports[i].pair.MasterAddr, r.transports[i].pair.SlaveAddr)
			r.transports[i].Close()
			r.transports[i] = CreateTransport(r, pairs[i])
			r.transports[i].Start()
			Logger.Printf("RPC\tReload %s: new endpoint %s <-> %s", r.name, r.transports[i].pair.MasterAddr, r.transports[i].pair.SlaveAddr)
		}
	}
	for i := len(r.transports); i < len(pairs); i++ {
		endpoint := CreateTransport(r, pairs[i])
		r.transports = append(r.transports, endpoint)
		endpoint.Start()
		Logger.Printf("RPC\tReload %s: new endpoint %s <-> %s", r.name, endpoint.pair.MasterAddr, endpoint.pair.SlaveAddr)
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

func (r *Remote) Allocate(stock int32, etag int32, handshake int32) int {
	r.transportMutex.RLock()
	defer r.transportMutex.RUnlock()
	if len(r.transports) == 0 {
		// Since we do not have any transports,
		// just do not allocate at all!
		return -1
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
	Logger.Printf("Remote\tMaster %s asked me for stock %d since trade no.%d reply with %d\n", r.name, stock, etag, bestK)
	r.transports[bestK].Allocate(stock, etag, handshake)
	return bestK
}

func (r *Remote) RecvLoop() {
	pending := make(map[int32]RemoteSubscribeRequest)
	pendingTimeout := make(chan int32)

	subscription := make(map[int32]chan *common.BLOrder)
	hsids := make(map[int32]int32)
	handshake := int32(0)

	allocations := make(map[int32]int)

	timeout := time.Duration(Config.SubscribeTimeoutMs) * time.Millisecond
	for {
		select {
		case dto := <-r.incoming:
			if common.IsCmd(dto.Mix) {
				cmd, payload := common.DecodeCmd(dto.Mix)
				switch cmd {
				case common.CmdSubReq: // Subscribe request, use payload as StkId, Price as etag, OrderId as handshake
					allocated := r.Allocate(payload, dto.Price, dto.OrderId)
					if allocated == -1 {
						allocations[payload] = allocated
					}

				case common.CmdSubRes: // Subscribe response, use OrderId as handshake
					if req, ok := pending[dto.OrderId]; ok {
						ch := make(chan *common.BLOrder)
						subscription[req.stock] = ch
						hsids[req.stock] = dto.OrderId
						req.result <- ch
						delete(pending, dto.OrderId)
					}

				case common.CmdUnsub:
					if k, ok := allocations[payload]; ok {
						r.transportMutex.RLock()
						if len(r.transports) > k { // Make sure we have that transport
							r.transports[k].Unallocate(payload)
						}
						r.transportMutex.RUnlock()
						delete(allocations, payload)
					}
				}
			} else {
				var order common.BLOrder
				common.UnmarshalOrderDTO(dto, &order)
				// 100ms data processing delay
				if ch, ok := subscription[order.StkCode]; ok {
					select {
					case ch <- &order:
					case <-time.After(time.Millisecond * 100):
						close(ch)
						delete(subscription, order.StkCode)
						r.command <- &common.BLTradeDTO{
							Mix:   common.EncodeCmd(common.CmdUnsub, order.StkCode),
							AskId: hsids[order.StkCode],
						}
					}
				} else {
					r.command <- &common.BLTradeDTO{
						Mix:   common.EncodeCmd(common.CmdUnsub, order.StkCode),
						AskId: hsids[order.StkCode],
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
		select {
		case <-time.After(interval):
		case <-r.reshape:
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
			Logger.Printf("Remote\tShaper master %s with transport %d", r.name, k)
			r.transports[k].Shape()
		}
		r.transportMutex.RUnlock()
	}
}

func (r *Remote) Start() {
	go r.MainLoop()
	go r.RecvLoop()
	go r.ShaperLoop()
}

// **NOTICE** return value might be nil
func (r *Remote) Subscribe(stock int32, etag int32) <-chan *common.BLOrder {
	Logger.Printf("Remote\tAsk master %s for stock %d since order no.%d\n", r.name, stock, etag)
	result := make(chan chan *common.BLOrder)
	r.subscribes <- RemoteSubscribeRequest{stock: stock, etag: etag, result: result}
	ch := <-result
	return ch
}
