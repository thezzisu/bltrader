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

type LocalSubscribeRequest struct {
	stock  int32
	etag   int32
	result chan chan *common.BLTrade
}

type RemotePacket struct {
	src  int
	data *common.BLTradeDTO
}

type LocalSubscription struct {
	sid   int16
	stock int32
	ch    chan *common.BLTrade
	src   int
}

type Remote struct {
	hub            *Hub
	manager        *common.RPCPairManager
	transports     []*Transport
	transportMutex sync.RWMutex
	name           string
	incoming       chan RemotePacket
	subscribes     chan LocalSubscribeRequest
	command        chan *common.BLOrderDTO
	reshape        chan struct{}
}

func CreateRemote(hub *Hub, name string) *Remote {
	configPath, _ := os.UserConfigDir()
	configPath = path.Join(configPath, "bltrader", fmt.Sprintf("rpc.%s.%s.json", Config.Name, name))

	r := new(Remote)
	r.hub = hub
	r.manager = common.CreateRPCPairManager(configPath)
	r.name = name
	r.incoming = make(chan RemotePacket, 128)
	r.subscribes = make(chan LocalSubscribeRequest)
	r.command = make(chan *common.BLOrderDTO, 16)
	r.reshape = make(chan struct{}, 16)
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
			r.transports[i] = CreateTransport(r, i, pairs[i])
			r.transports[i].Start()
			Logger.Printf("Remote\tReload %s: new endpoint %s <-> %s", r.name, r.transports[i].pair.MasterAddr, r.transports[i].pair.SlaveAddr)
		}
	}
	for i := len(r.transports); i < len(pairs); i++ {
		endpoint := CreateTransport(r, i, pairs[i])
		r.transports = append(r.transports, endpoint)
		endpoint.Start()
		Logger.Printf("Remote\tReload %s: new endpoint %s <-> %s", r.name, endpoint.pair.MasterAddr, endpoint.pair.SlaveAddr)
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

func (r *Remote) Allocate(stock int32, etag int32, sid int16) int {
	ch := r.hub.stocks[stock].Subscribe(etag)
	if ch == nil {
		r.command <- &common.BLOrderDTO{
			Sid:     -common.CmdSubRes,
			Volume:  sid,
			OrderId: 0,
		}
		return -1
	}
	r.transportMutex.RLock()
	defer r.transportMutex.RUnlock()
	if len(r.transports) == 0 {
		// Since we do not have any transports,
		// just do not allocate at all!
		return -1
	}
	bestK, bestV, bestW := 0, atomic.LoadInt32(&r.transports[0].subscriptionCount), atomic.LoadInt32(&r.transports[0].pendingCount)
	for i := 1; i < len(r.transports); i++ {
		v := atomic.LoadInt32(&r.transports[i].subscriptionCount)
		w := atomic.LoadInt32(&r.transports[i].pendingCount)
		if v < bestV || v == bestV && w < bestW {
			bestK, bestV, bestW = i, v, w
		} else if v == bestV && w == bestW && rand.Int()&1 == 0 {
			bestK = i
		}
	}
	// Logger.Printf("Remote\tSlave %s asked me for stock %d since order no.%d reply with %d\n", r.name, stock, etag, bestK)
	r.transports[bestK].Add(stock, sid, ch)
	return bestK
}

func (r *Remote) RecvLoop() {
	pending := make(map[int16]LocalSubscribeRequest)
	stockMap := make(map[int32]int16)
	subscription := make(map[int16]LocalSubscription)
	allocation := make(map[int16]int)
	expired := make(chan int16)
	sidGen := int16(0)
	nextSid := func() int16 {
		sidGen++
		if sidGen <= 0 {
			sidGen = 1
		}
		return sidGen
	}

	subscribeTimeout := time.Duration(Config.SubscribeTimeoutMs) * time.Millisecond
	processTimeout := time.Duration(Config.ProcessTimeoutMs) * time.Millisecond
	for {
		select {
		case packet := <-r.incoming:
			dto := packet.data
			if dto.Sid < 0 {
				cmd := -dto.Sid
				switch cmd {
				case common.CmdSubReq: // Subscribe request, use payload as StkId, Price as etag, AskId as handshake
					stock := dto.AskId
					etag := dto.Price
					sid := dto.Volume
					allocated := r.Allocate(stock, etag, sid)
					if allocated != -1 {
						allocation[sid] = allocated
					}

				case common.CmdSubRes: // Subscribe response, use AskId as handshake
					sid := dto.Volume
					retry := dto.AskId == 0
					if req, ok := pending[sid]; ok {
						if retry {
							req.result <- nil
						} else {
							ch := make(chan *common.BLTrade, 1000000)
							subscription[sid] = LocalSubscription{
								sid:   sid,
								stock: req.stock,
								ch:    ch,
								src:   packet.src,
							}
							stockMap[req.stock] = sid
							req.result <- ch
						}
						delete(pending, sid)
					}

				case common.CmdUnsub: // Unsubscribe request
					sid := dto.Volume
					if k, ok := allocation[sid]; ok {
						r.transportMutex.RLock()
						if len(r.transports) > k { // Make sure we have that transport
							r.transports[k].Unallocate(sid)
						}
						r.transportMutex.RUnlock()
						delete(allocation, sid)
					}

				case common.CmdPeekRes: // Peek response
					volume := dto.Volume
					if volume != -1 {
						stock := dto.AskId
						id := dto.BidId
						r.hub.stocks[stock].TradeHook(id, int32(volume))
					}
				}
			} else {
				sid := dto.Sid
				if sub, ok := subscription[sid]; ok && sub.src == packet.src && sub.sid == sid {
					var trade common.BLTrade
					common.UnmarshalTradeDTO(sub.stock, dto, &trade)
					timer := time.NewTimer(processTimeout)
					select {
					case sub.ch <- &trade:
						if !timer.Stop() {
							<-timer.C
						}

					case <-timer.C:
						close(sub.ch)
						delete(subscription, sid)
						r.command <- &common.BLOrderDTO{
							Mix:    -common.CmdUnsub,
							Volume: sid,
						}
					}
				}
			}

		case hs := <-expired:
			if req, ok := pending[hs]; ok {
				req.result <- nil
				delete(pending, hs)
			}

		case req := <-r.subscribes:
			if sid, ok := stockMap[req.stock]; ok {
				if sub, ok := subscription[sid]; ok {
					close(sub.ch)
					delete(subscription, sid)
				}
			}

			sid := nextSid()
			pending[sid] = req
			r.command <- &common.BLOrderDTO{
				Sid:     -common.CmdSubReq,
				OrderId: req.stock,
				Price:   req.etag,
				Volume:  sid,
			}
			go func(sid int16) {
				time.Sleep(subscribeTimeout)
				expired <- sid
			}(sid)
		}
	}
}

func (r *Remote) ShaperLoop() {
	interval := time.Millisecond * time.Duration(Config.ShaperIntervalMs)
	for {
		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
		case <-r.reshape:
			if !timer.Stop() {
				<-timer.C
			}
		}
		r.transportMutex.RLock()

		min, max := int32(math.MaxInt32), int32(0)
		k := 0
		loadStr := ""
		for i, t := range r.transports {
			count := atomic.LoadInt32(&t.subscriptionCount)
			loadStr += fmt.Sprintf(" %d", count)
			if count < min {
				min = count
			}
			if count > max {
				max = count
				k = i
			}
		}
		Logger.Printf("Remote\tShaper %s load\033[34m%s\033[0m", r.name, loadStr)
		if max >= 2 && max-min > 0 {
			Logger.Printf("Remote\tShaper %s shape transport %d", r.name, k)
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
func (r *Remote) Subscribe(stock int32, etag int32) <-chan *common.BLTrade {
	Logger.Printf("Remote\tAsk slave \033[33m%s\033[0m for stock \033[33m%d\033[0m since trade \033[33m%d\033[0m\n", r.name, stock, etag)
	result := make(chan chan *common.BLTrade)
	r.subscribes <- LocalSubscribeRequest{stock: stock, etag: etag, result: result}
	ch := <-result
	return ch
}

func (r *Remote) RequestPeek(stock int32, id int32) {
	Logger.Printf("Remote\tPeek slave \033[33m%s\033[0m stock \033[33m%d\033[0m trade \033[33m%d\033[0m\n", r.name, stock, id)
	r.command <- &common.BLOrderDTO{
		Sid:     -common.CmdPeekReq,
		OrderId: stock,
		Price:   id,
	}
}
