package lib

import (
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/thezzisu/bltrader/common"
	"github.com/thezzisu/bltrader/core"
)

var ChunkSize int

type TradeStore struct {
	offset int
	cacheL []common.BLTrade
	cacheR []common.BLTrade
	mutex  sync.RWMutex
}

func CreateTradeStore() *TradeStore {
	return &TradeStore{
		cacheL: make([]common.BLTrade, 0),
		cacheR: make([]common.BLTrade, 0),
	}
}

func (ts *TradeStore) Append(adata []common.BLTrade) {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

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
	t.ts.mutex.RLock()
	defer t.ts.mutex.RUnlock()

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
	t.ts.mutex.RLock()
	defer t.ts.mutex.RUnlock()

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
	datas      map[string]chan *common.BLOrder
	readers    map[string]*TradeReader
	tradest    *TradeStore
}

func CreateStockHandler(hub *Hub, stockId int32) *StockHandler {
	sh := new(StockHandler)
	sh.hub = hub
	sh.stockId = stockId
	sh.subscribes = make(map[string]chan *StockSubscribeRequest)
	sh.datas = make(map[string]chan *common.BLOrder)
	sh.readers = make(map[string]*TradeReader)
	return sh
}

func (sh *StockHandler) Subscribe(name string, etag int32) <-chan *common.BLTradeDTO {
	ch := make(chan *common.BLTradeDTO)
	timer := time.NewTimer(time.Millisecond * 100)
	select {
	case sh.subscribes[name] <- &StockSubscribeRequest{etag: etag, ch: ch}:
		if !timer.Stop() {
			<-timer.C
		}
		return ch

	case <-timer.C:
		close(ch)
		return nil
	}
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
	data := sh.datas[name]
	etag := int32(0)
	timeout := time.Millisecond * time.Duration(Config.StockHandlerTimeoutMs)
subscribe:
	for {
		ch := remote.Subscribe(sh.stockId, etag)
		if ch == nil {
			continue
		}
		for {
			timer := time.NewTimer(timeout)
			select {
			case order, ok := <-ch:
				if !timer.Stop() {
					<-timer.C
				}
				if !ok {
					break
				}
				if order.OrderId == -1 {
					break subscribe
				}
				etag = order.OrderId
				data <- order

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

	n := len(sh.datas)
	caches := make([]*common.BLOrder, n)
	sources := make([]chan *common.BLOrder, n)
	cases := make([]reflect.SelectCase, n)
	locs := make([]int, n)
	i := 0
	for _, data := range sh.datas {
		caches[i] = nil
		sources[i] = data
		i++
	}

	remove := func(pos int) {
		sources[pos] = sources[n-1]
		sources = sources[:n-1]
		caches[pos] = caches[n-1]
		caches = caches[:n-1]
		n--
	}

	ready := func() bool {
		for _, v := range caches {
			if v == nil {
				return false
			}
		}
		return true
	}

	for {
		for !ready() {
			i = 0
			for j := 0; j < n; j++ {
				if caches[j] == nil {
					cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(sources[j])}
					locs[i] = j
					i++
				}
			}
			chosen, recv, ok := reflect.Select(cases[:i])
			if ok {
				caches[locs[chosen]] = recv.Interface().(*common.BLOrder)
			} else {
				remove(locs[chosen])
			}
		}
		k, v := -1, int32(math.MaxInt32)
		for i := 0; i < n; i++ {
			if caches[i].OrderId < v {
				k, v = i, caches[i].OrderId
			}
		}
		if k == -1 {
			break
		}
		ord := caches[k]
		caches[k] = nil
		if sh.stockId == 0 {
			Logger.Println(k, ord.OrderId)
		}
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
		sh.datas[master.Name] = make(chan *common.BLOrder, 1000000)
		sh.readers[master.Name] = CreateTradeReader(sh.tradest)

		sh.hub.wg.Add(1)
		go sh.SendLoop(master.Name)
		go sh.RecvLoop(master.Name)
	}
	go sh.MergeLoop()
}
