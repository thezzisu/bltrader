package lib

import (
	"time"

	"github.com/thezzisu/bltrader/common"
	"github.com/thezzisu/bltrader/core"
)

type Peeker struct {
	ch   chan *common.BLOrder
	last *common.BLOrder
}

func CreatePeeker() *Peeker {
	ch := make(chan *common.BLOrder)
	return &Peeker{ch: ch}
}

func (p *Peeker) Peek() *common.BLOrder {
	if p.last == nil {
		p.last = <-p.ch
	}
	return p.last
}

func (p *Peeker) Get() *common.BLOrder {
	if p.last == nil {
		return <-p.ch
	}
	last := p.last
	p.last = nil
	return last
}

var ChunkSize int

type TradeStore struct {
	offset int
	cacheL []common.BLTrade
	cacheR []common.BLTrade
}

func CreateTradeStore() *TradeStore {
	return &TradeStore{
		cacheL: make([]common.BLTrade, 0),
		cacheR: make([]common.BLTrade, 0),
	}
}

func (ts *TradeStore) Append(adata []common.BLTrade) {
	ts.cacheR = append(ts.cacheR, adata...)
	if len(ts.cacheR) > ChunkSize {
		ts.cacheL = ts.cacheR[0:ChunkSize]
		ts.cacheR = ts.cacheR[ChunkSize:]
		ts.offset++
	}
}

type TradeReader struct {
	ts     *TradeStore
	offset int
	ptr    int
	incR   bool
}

func CreateTradeReader(ts *TradeStore) *TradeReader {
	t := new(TradeReader)
	t.ts = ts
	t.ptr = 0
	t.incR = true
	return t
}

func (t *TradeReader) Seek(etag int32) {
	//find the first pos >= etag
	if t.offset*ChunkSize > int(etag) {
		t.incR = false
	}
	if t.offset*ChunkSize+len(t.ts.cacheR) <= int(etag) {
		t.ptr = len(t.ts.cacheR)
		t.incR = true
		return
	}
	// [offset - 1 * C,offset * C)
	// [offset * C,offset * C + len)
	if t.incR {
		t.ptr = int(etag) - t.offset*ChunkSize
	} else {
		t.ptr = int(etag) - (t.offset-1)*ChunkSize
	}
}

func (t *TradeReader) Next() *common.BLTrade {
	if !t.incR {
		if t.ptr == len(t.ts.cacheL) {
			t.incR = true
			t.ptr = 0
			return &t.ts.cacheR[0]
		}
		t.ptr++
		return &t.ts.cacheL[t.ptr-1]
	}
	if t.ptr == len(t.ts.cacheR) {
		return nil
	}
	t.ptr++
	return &t.ts.cacheR[t.ptr-1]
}

type StockSubscribeRequest struct {
	etag int32
	ch   chan *common.BLTradeDTO
}

type StockHandler struct {
	hub        *Hub
	stockId    int32
	subscribes map[string]chan *StockSubscribeRequest
	peekers    map[string]*Peeker
	readers    map[string]*TradeReader
	tradest    *TradeStore
}

func CreateStockHandler(hub *Hub, stockId int32) *StockHandler {
	sh := new(StockHandler)
	sh.hub = hub
	sh.stockId = stockId
	sh.subscribes = make(map[string]chan *StockSubscribeRequest)
	sh.peekers = make(map[string]*Peeker)
	sh.readers = make(map[string]*TradeReader)
	return sh
}

func (sh *StockHandler) Subscribe(name string, etag int32) <-chan *common.BLTradeDTO {
	ch := make(chan *common.BLTradeDTO)
	sh.subscribes[name] <- &StockSubscribeRequest{etag: etag, ch: ch}
	return ch
}

func (sh *StockHandler) SendLoop(name string) {
	ch := make(chan *common.BLTradeDTO)
	subscribe := sh.subscribes[name]
	reader := sh.readers[name]

	replace := func(req *StockSubscribeRequest) {
		Logger.Printf("StockHandler[%d].SendLoop(%s): master subscribed since %d\n", sh.stockId, name, req.etag)
		close(ch)
		ch = req.ch
		reader.Seek(req.etag)
	}

	for {
		trade := reader.Next()

		if trade == nil {
			// Send finished
			// Write EOF to remote
			dto := new(common.BLTradeDTO)
			common.MarshalTradeDTO(&common.BLTrade{
				StkCode: sh.stockId,
				AskId:   -1,
			}, dto)

			select {
			// New subscriber
			case req := <-subscribe:
				replace(req)

			// EOF sent, waiting for new subscriber
			case ch <- dto:
				req := <-subscribe
				replace(req)
			}
			continue
		}

		dto := new(common.BLTradeDTO)
		common.MarshalTradeDTO(trade, dto)

		select {
		case req := <-subscribe:
			replace(req)
		case ch <- dto:
		}
	}
}

func (sh *StockHandler) RecvLoop(name string) {
	remote := sh.hub.remotes[name]
	peeker := sh.peekers[name]
	etag := int32(0)
subscribe:
	for {
		ch := remote.Subscribe(sh.stockId, etag)
		for {
			// TODO add configuration for timeout
			timer := time.NewTimer(time.Second * 10)
			select {
			case order, ok := <-ch:
				if !ok {
					break
				}
				if order.OrderId == -1 {
					break subscribe
				}
				etag = order.OrderId
				peeker.ch <- order

			case <-timer.C:
				Logger.Printf("StockHandler[%d].RecvLoop(%s) timeout\n", sh.stockId, name)
				continue subscribe
			}
		}
	}
	Logger.Printf("StockHandler[%d].RecvLoop done\n", sh.stockId)
	sh.hub.wg.Done()
}

func (sh *StockHandler) MergeLoop() {
	blr := new(core.BLRunner)
	lower, upper := -10000.0, 10000.0
	//TODO: idk where to find bounds
	blr.Load(lower, upper)
	for {
		key := ""
		v := int32(2000000)
		for k, pk := range sh.peekers {
			u := pk.Peek()
			if u == nil {
				continue
			}
			if u.OrderId < v {
				key, v = k, u.OrderId
			}
		}
		if key == "" {
			break
		}
		ord := sh.peekers[key].Get()
		trades := blr.Dispatch(ord)
		sh.tradest.Append(trades)
	}
	Logger.Printf("StockHandler[%d].MergeLoop done\n", sh.stockId)
}

func (sh *StockHandler) Start() {
	ChunkSize = 1000000
	sh.tradest = CreateTradeStore()
	for _, master := range Config.Masters {
		sh.subscribes[master.Name] = make(chan *StockSubscribeRequest)
		sh.peekers[master.Name] = CreatePeeker()
		sh.readers[master.Name] = CreateTradeReader(sh.tradest)

		sh.hub.wg.Add(1)
		go sh.SendLoop(master.Name)
		go sh.RecvLoop(master.Name)
	}
	go sh.MergeLoop()
}
