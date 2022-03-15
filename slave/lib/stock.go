package lib

import (
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
	fetch  sync.Mutex
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
	ts.fetch.Lock()
	defer ts.fetch.Unlock()

	if ts.last >= id {
		return
	}
	for ts.last < id && !ts.eod {
		dto, ok := <-ts.source
		ts.mutex.Lock()
		if !ok {
			ts.eod = true
		} else {
			ts.offset = ts.offset + 1
			if ts.offset >= ts.size {
				ts.offset = 0
			}
			ts.cache[ts.offset] = dto
			ts.last++
		}
		ts.mutex.Unlock()
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
	C     chan *common.BLTradeDTO
	cmd   chan struct{}
	die   chan struct{}
}

func CreateTradeReader(store *TradeStore, etag int32) *TradeReader {
	t := new(TradeReader)
	t.store = store
	t.ptr = etag
	t.C = make(chan *common.BLTradeDTO)
	t.cmd = make(chan struct{})
	t.die = make(chan struct{})
	return t
}

func (t *TradeReader) FetchLoop() {
	for {
		select {
		case <-t.die:
			close(t.cmd)
			close(t.C)
			return
		case <-t.cmd:
		}
		t.ptr++
		dto := t.store.Get(t.ptr)
		select {
		case <-t.die:
			close(t.cmd)
			close(t.C)
			return
		case t.C <- dto:
		}
	}
}

func (t *TradeReader) Close() {
	close(t.die)
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
}

func CreateStockHandler(hub *Hub, stockId int32) *StockHandler {
	sh := new(StockHandler)
	sh.hub = hub
	sh.stockId = stockId
	sh.subscribes = make(map[string]chan *StockSubscribeRequest)
	sh.datas = make(map[string]chan *common.BLOrder)
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
	subscribe := sh.subscribes[name]
	var ch chan *common.BLTradeDTO
	var reader *TradeReader

	replace := func(req *StockSubscribeRequest, eager bool) {
		Logger.Printf("Stock %d\tmaster %s subscribed since %d current %d\n", sh.stockId, name, req.etag, sh.store.last)
		if !eager {
			close(ch)
			reader.Close()
		}
		ch = req.ch
		reader = CreateTradeReader(sh.store, req.etag)
		go reader.FetchLoop()
	}

	replace(<-subscribe, true)

	for {
		select {
		case reader.cmd <- struct{}{}:
		case req := <-subscribe:
			replace(req, false)
			continue
		}

		var dto *common.BLTradeDTO
		select {
		case dto = <-reader.C:
		case req := <-subscribe:
			replace(req, false)
			continue
		}

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
				replace(req, false)

			// EOF sent, waiting for new subscriber
			case ch <- dto:
				close(ch)
				reader.Close()
				replace(<-subscribe, true)
			}
			continue
		}

		select {
		case req := <-subscribe:
			replace(req, false)
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

	caches := make([]*common.BLOrder, 0)
	sources := make([]chan *common.BLOrder, 0)
	for _, data := range sh.datas {
		caches = append(caches, nil)
		sources = append(sources, data)
	}
	n := len(caches)
	remove := func(pos int) {
		sources[pos] = sources[n-1]
		sources = sources[:n-1]
		caches[pos] = caches[n-1]
		caches = caches[:n-1]
		n--
	}

	cases := make([]reflect.SelectCase, n)
	locs := make([]int, n)
	load := func() {
		m := 0
		for i := 0; i < n; i++ {
			if caches[i] == nil {
				cases[m] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(sources[i])}
				locs[m] = i
				m++
			}
		}
		chosen, recv, ok := reflect.Select(cases[:m])
		if ok {
			caches[locs[chosen]] = recv.Interface().(*common.BLOrder)
		} else {
			remove(locs[chosen])
		}
	}

	lastId := int32(0)
	next := func() *common.BLOrder {
		for k, v := range caches {
			if v != nil && v.OrderId == lastId+1 {
				caches[k] = nil
				lastId++
				return v
			}
		}
		return nil
	}

	for n > 0 {
		order := next()
		for n > 0 && order == nil {
			load()
			order = next()
		}
		if order == nil {
			if n > 0 {
				Logger.Fatalf("Stock %d\tMergeLoop: no order found\n", sh.stockId)
			}
			break
		}

		trades := blr.Dispatch(order)
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

		sh.hub.wg.Add(1)
		go sh.SendLoop(master.Name)
		go sh.RecvLoop(master.Name)
	}
	sh.hub.wg.Add(1)
	go sh.MergeLoop()
}
