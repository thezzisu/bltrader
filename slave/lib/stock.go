package lib

import (
	"reflect"
	"sync"
	"time"

	"github.com/thezzisu/bltrader/common"
	"github.com/thezzisu/bltrader/core"
)

type BLTradeComp struct {
	BidId  int32
	AskId  int32
	Price  int32
	Volume int16
}

type TradeStore struct {
	size   int               // cache size
	source chan *BLTradeComp // data source
	cache  []*BLTradeComp    // Use DTO to reduce memory usage
	offset int               // index of last trade in cache
	last   int32             // the id of last trade
	eod    bool              // flag to indicate the end of data
	mutex  sync.RWMutex
	fetch  sync.Mutex
}

func CreateTradeStore(size int) *TradeStore {
	ts := new(TradeStore)
	ts.size = size
	ts.source = make(chan *BLTradeComp, size)
	ts.cache = make([]*BLTradeComp, size)
	ts.offset = -1
	ts.last = 0
	ts.eod = false
	return ts
}

func (ts *TradeStore) Close() {
	close(ts.source)
}

func (ts *TradeStore) Last() int32 {
	ts.mutex.RLock()
	defer ts.mutex.RUnlock()
	return ts.last
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

func (ts *TradeStore) TryGet(id int32) (*BLTradeComp, bool) {
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

func (ts *TradeStore) Get(id int32) *BLTradeComp {
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
	C     chan *BLTradeComp
	cmd   chan struct{}
	die   chan struct{}
}

func CreateTradeReader(store *TradeStore, etag int32) *TradeReader {
	t := new(TradeReader)
	t.store = store
	t.ptr = etag
	t.C = make(chan *BLTradeComp)
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
	etag   int32
	result chan chan *BLTradeComp
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

func (sh *StockHandler) Subscribe(name string, etag int32) <-chan *BLTradeComp {
	result := make(chan chan *BLTradeComp)
	sh.subscribes[name] <- &StockSubscribeRequest{etag, result}
	ch := <-result
	return ch
}

func (sh *StockHandler) SendLoop(name string) {
	subscribe := sh.subscribes[name]
	var ch chan *BLTradeComp
	var reader *TradeReader

	replace := func(req *StockSubscribeRequest, eager bool) {
		Logger.Printf("Stock \033[33m%d\033[0m\tmaster \033[33m%s\033[0m subscribed since \033[32m%d\033[0m current \033[32m%d\033[0m\n", sh.stockId, name, req.etag, sh.store.last)
		if !eager {
			close(ch)
			reader.Close()
		}
		ch = make(chan *BLTradeComp)
		req.result <- ch
		reader = CreateTradeReader(sh.store, req.etag)
		go reader.FetchLoop()
	}

	replace(<-subscribe, true)

subscribeLoop:
	for {
		// Since we just subscribed, reader's fetchloop isn't blocked
		reader.cmd <- struct{}{}

		var dto *BLTradeComp
	readLoop:
		for {
			select {
			case dto = <-reader.C:
				break readLoop

			case req := <-subscribe:
				if req.etag == sh.store.Last() {
					Logger.Printf("Stock \033[33m%d\033[0m\tmaster \033[33m%s\033[0m subscribed since \033[31m%d\033[0m current \033[31m%d\033[0m\n", sh.stockId, name, req.etag, sh.store.last)
					req.result <- nil
				} else {
					replace(req, false)
					continue subscribeLoop
				}
			}
		}

		if dto == nil {
			// Send finished
			// Write EOF to remote
			comp := &BLTradeComp{
				AskId: -1,
			}

			select {
			// New subscriber
			case req := <-subscribe:
				replace(req, false)

			// EOF sent, waiting for new subscriber
			case ch <- comp:
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
	// f, _ := os.Create(fmt.Sprintf("stock-%d-%s.txt", sh.stockId, name))

subscribe:
	for {
		ch := remote.Subscribe(sh.stockId, etag)
		// fmt.Fprintf(f, "Subscribed since %d\n", etag)
		if ch == nil {
			time.Sleep(timeout)
			Logger.Printf("Stock \033[33m%d\033[0m\tRecvLoop (\033[33m%s\033[0m) RETRY\n", sh.stockId, name)
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
					Logger.Printf("Stock \033[33m%d\033[0m\tRecvLoop (\033[33m%s\033[0m) DIE\n", sh.stockId, name)
					continue subscribe
				}
				if order.OrderId == -1 {
					break subscribe
				}
				etag = order.OrderId
				// fmt.Fprintf(f, "%d %d %d %d %f %d\n", order.StkCode, order.OrderId, order.Direction, order.Type, order.Price, order.Volume)
				data <- order

			case <-timer.C:
				Logger.Printf("Stock \033[33m%d\033[0m\tRecvLoop (\033[33m%s\033[0m) TIMEOUT\n", sh.stockId, name)
				continue subscribe
			}
		}
	}
	Logger.Printf("Stock \033[33m%d\033[0m\tRecvLoop (\033[33m%s\033[0m) done\n", sh.stockId, name)
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
		if m == 0 {
			Logger.Printf("%d %d\n", caches[0].OrderId, caches[1].OrderId)
			Logger.Fatalf("Stock \033[33m%d\033[0m\tMergeLoop no data", sh.stockId)
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

	// f, _ := os.Create(fmt.Sprintf("stock-%d.txt", sh.stockId))
	// g, _ := os.Create(fmt.Sprintf("trade-%d.txt", sh.stockId))
	for n > 0 {
		order := next()
		for n > 0 && order == nil {
			load()
			order = next()
		}
		if order == nil {
			break
		}

		// fmt.Fprintf(f, "%d %d %d %d %f %d\n", order.StkCode, order.OrderId, order.Direction, order.Type, order.Price, order.Volume)

		if order.Volume != 0 {
			trades := blr.Dispatch(order)
			for _, trade := range trades {
				// fmt.Fprintf(g, "%d %d %d %f %d\n", trade.StkCode, trade.AskId, trade.BidId, trade.Price, trade.Volume)
				sh.store.source <- &BLTradeComp{
					BidId:  trade.BidId,
					AskId:  trade.AskId,
					Price:  common.PriceF2I(trade.Price),
					Volume: int16(trade.Volume),
				}
			}
		}
	}

	sh.store.Close()
	Logger.Printf("Stock \033[33m%d\033[0m\tMergeLoop done\n", sh.stockId)
	sh.hub.wg.Done()
}

func (sh *StockHandler) Start() {
	cacheSize := Config.TradeStoreSize
	sh.store = CreateTradeStore(cacheSize)
	for _, master := range Config.Masters {
		sh.subscribes[master.Name] = make(chan *StockSubscribeRequest)
		sh.datas[master.Name] = make(chan *common.BLOrder, 10000000)

		sh.hub.wg.Add(1)
		go sh.SendLoop(master.Name)
		go sh.RecvLoop(master.Name)
	}
	sh.hub.wg.Add(1)
	go sh.MergeLoop()
}
