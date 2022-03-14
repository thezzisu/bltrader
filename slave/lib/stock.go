package lib

import (
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/thezzisu/bltrader/common"
	"github.com/thezzisu/bltrader/core"
)

type TradeStore struct {
	size   int                     // cache size
	source chan *common.BLTradeDTO // data source
	cache  []*common.BLTradeDTO    // Use DTO to reduce memory usage
	offset int                     // index of last trade in cache
	last   int32                   // the id of last trade
	eod    bool                    // flag to indicate the end of data
	mutex  sync.RWMutex
}

func CreateTradeStore(size int) *TradeStore {
	ts := new(TradeStore)
	ts.size = size
	ts.source = make(chan *common.BLTradeDTO, size)
	ts.cache = make([]*common.BLTradeDTO, size)
	ts.offset = -1
	ts.last = 0
	ts.eod = false
	return ts
}

func (ts *TradeStore) Close() {
	close(ts.source)
}

func (ts *TradeStore) Ensure(id int32) {
	ts.mutex.Lock()
	defer ts.mutex.Unlock()

	if ts.last >= id {
		return
	}
	for ts.last < id {
		dto, ok := <-ts.source
		if !ok {
			ts.eod = true
			break
		}
		ts.offset = ts.offset + 1
		if ts.offset >= ts.size {
			ts.offset = 0
		}
		ts.cache[ts.offset] = dto
		ts.last++
	}
}

func (ts *TradeStore) TryGet(id int32) (*common.BLTradeDTO, bool) {
	ts.mutex.RLock()
	defer ts.mutex.RUnlock()

	if ts.last >= id {
		loc := ts.offset - int(ts.last-id)
		if loc < 0 {
			loc = loc + ts.size
		}
		return ts.cache[loc], true
	}
	if ts.eod {
		return nil, true
	}
	return nil, false
}

func (ts *TradeStore) Get(id int32) *common.BLTradeDTO {
	dto, ok := ts.TryGet(id)
	if ok {
		return dto
	}
	ts.Ensure(id)
	dto, _ = ts.TryGet(id)
	return dto
}

type TradeReader struct {
	store *TradeStore
	ptr   int32 // self offset
}

func CreateTradeReader(store *TradeStore) *TradeReader {
	t := new(TradeReader)
	t.store = store
	t.ptr = 0
	return t
}

func (t *TradeReader) Seek(etag int32) {
	t.ptr = etag
}

func (t *TradeReader) Next() *common.BLTradeDTO {
	t.ptr++
	return t.store.Get(t.ptr)
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
	store      *TradeStore
	readers    map[string]*TradeReader
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
		Logger.Printf("Stock %d\tmaster %s subscribed since %d\n", sh.stockId, name, req.etag)
		close(ch)
		ch = req.ch
		reader.Seek(req.etag)
	}

	for {
		dto := reader.Next()

		if dto == nil {
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
				Logger.Printf("Stock %d\tRecvLoop (%s) timeout\n", sh.stockId, name)
				continue subscribe
			}
		}
	}
	Logger.Printf("Stock %d\tRecvLoop (%s) done\n", sh.stockId, name)
	close(data)
	sh.hub.wg.Done()
}

func (sh *StockHandler) MergeLoop() {
	blr := new(core.BLRunner)
	blr.Load()

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

		trades := blr.Dispatch(ord)
		for _, trade := range trades {
			var dto common.BLTradeDTO
			common.MarshalTradeDTO(&trade, &dto)
			sh.store.source <- &dto
		}
	}

	sh.store.Close()
	Logger.Printf("Stock %d\tMergeLoop done\n", sh.stockId)
	sh.hub.wg.Done()
}

func (sh *StockHandler) Start() {
	cacheSize := 1000000
	sh.store = CreateTradeStore(cacheSize)
	for _, master := range Config.Masters {
		sh.subscribes[master.Name] = make(chan *StockSubscribeRequest)
		sh.datas[master.Name] = make(chan *common.BLOrder, 1000000)
		sh.readers[master.Name] = CreateTradeReader(sh.store)

		sh.hub.wg.Add(1)
		go sh.SendLoop(master.Name)
		go sh.RecvLoop(master.Name)
	}
	sh.hub.wg.Add(1)
	go sh.MergeLoop()
}
