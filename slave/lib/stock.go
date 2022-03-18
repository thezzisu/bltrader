package lib

import (
	"reflect"
	"sync"
	"time"

	"github.com/thezzisu/bltrader/common"
	"github.com/thezzisu/bltrader/core"
)

type TradeStore struct {
	size     int                       // cache size
	overflow int                       // overflow size
	source   chan []common.BLTradeComp // data source
	cache    []*common.BLTradeComp     // Use DTO to reduce memory usage
	offset   int                       // index of last trade in cache
	last     int32                     // the id of last trade produced
	tag      int32                     // the id of last trade consumed
	eod      bool                      // flag to indicate the end of data
	mutex    sync.RWMutex
	tagMutex sync.RWMutex
}

func CreateTradeStore(size int) *TradeStore {
	ts := new(TradeStore)
	ts.size = size * 2
	ts.overflow = size
	ts.source = make(chan []common.BLTradeComp)
	ts.cache = make([]*common.BLTradeComp, ts.size)
	ts.offset = -1
	ts.last = 0
	ts.tag = 0
	ts.eod = false
	return ts
}

func (ts *TradeStore) Close() {
	close(ts.source)
}

func (ts *TradeStore) HandleLoop() {
	spinTimeout := time.Millisecond * 100
	for {
		for int(ts.last-ts.tag) > ts.overflow {
			time.Sleep(spinTimeout)
		}

		trades, ok := <-ts.source
		if ok {
			ts.mutex.Lock()
			for _, trade := range trades {
				tradeComp := new(common.BLTradeComp)
				tradeComp.BidId = trade.BidId
				tradeComp.AskId = trade.AskId
				tradeComp.Price = trade.Price
				tradeComp.Volume = trade.Volume
				ts.offset = ts.offset + 1
				if ts.offset >= ts.size {
					ts.offset = 0
				}
				ts.cache[ts.offset] = tradeComp
				ts.last++
			}
			ts.mutex.Unlock()
		} else {
			ts.mutex.Lock()
			ts.eod = true
			ts.mutex.Unlock()
			break
		}
	}
}

func (ts *TradeStore) Last() int32 {
	ts.mutex.RLock()
	defer ts.mutex.RUnlock()
	return ts.last
}

func (ts *TradeStore) Tag(tag int32) {
	ts.tagMutex.Lock()
	if tag > ts.tag {
		ts.tag = tag
	}
	ts.tagMutex.Unlock()
}

func (ts *TradeStore) TryGet(id int32) (*common.BLTradeComp, bool) {
	ts.mutex.RLock()
	defer ts.mutex.RUnlock()

	if ts.last >= id {
		loc := ts.offset - int(ts.last-id)
		if loc < 0 {
			loc = loc + ts.size
		}
		ts.Tag(id)
		return ts.cache[loc], true
	}
	if ts.eod {
		return nil, true
	}
	return nil, false
}

type StockSubscribeRequest struct {
	etag   int32
	result chan chan *common.BLTradeComp
}

type StockHandler struct {
	hub        *Hub
	stockId    int32
	subscribes map[string]chan *StockSubscribeRequest
	datas      map[string]chan *common.BLOrderComp
	store      *TradeStore
}

func CreateStockHandler(hub *Hub, stockId int32) *StockHandler {
	sh := new(StockHandler)
	sh.hub = hub
	sh.stockId = stockId
	sh.subscribes = make(map[string]chan *StockSubscribeRequest)
	sh.datas = make(map[string]chan *common.BLOrderComp)
	return sh
}

func (sh *StockHandler) Subscribe(name string, etag int32) <-chan *common.BLTradeComp {
	result := make(chan chan *common.BLTradeComp)
	sh.subscribes[name] <- &StockSubscribeRequest{etag, result}
	ch := <-result
	return ch
}

func (sh *StockHandler) SendLoop(name string) {
	subscribe := sh.subscribes[name]
	var ch chan *common.BLTradeComp
	var ptr int32
	spinTimeout := time.Millisecond * 100

	replace := func(req *StockSubscribeRequest, eager bool) {
		Logger.Printf("Stock \033[33m%d\033[0m\tmaster \033[33m%s\033[0m subscribed since \033[32m%d\033[0m current \033[32m%d\033[0m\n", sh.stockId, name, req.etag, sh.store.last)
		if !eager {
			close(ch)
		}
		ch = make(chan *common.BLTradeComp)
		req.result <- ch
		ptr = req.etag
	}

	replace(<-subscribe, true)

subscribeLoop:
	for {
		// Since we just subscribed, reader's fetchloop isn't blocked
		ptr++
		dto, ok := sh.store.TryGet(ptr)
		for !ok {
			timer := time.NewTimer(spinTimeout)
			select {
			case req := <-subscribe:
				if !timer.Stop() {
					<-timer.C
				}
				if req.etag == sh.store.Last() {
					Logger.Printf("Stock \033[33m%d\033[0m\tmaster \033[33m%s\033[0m subscribed since \033[31m%d\033[0m current \033[31m%d\033[0m\n", sh.stockId, name, req.etag, sh.store.last)
					req.result <- nil
				} else {
					replace(req, false)
					continue subscribeLoop
				}

			case <-timer.C:
				dto, ok = sh.store.TryGet(ptr)
			}
		}

		if dto == nil {
			// Send finished
			// Write EOF to remote
			comp := &common.BLTradeComp{
				AskId: -1,
			}

			select {
			// New subscriber
			case req := <-subscribe:
				replace(req, false)

			// EOF sent, waiting for new subscriber
			case ch <- comp:
				close(ch)
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
	bufferSize := cap(data)
	limit := bufferSize / 2
	// 100ms wait timeout
	waitTimeout := time.Millisecond * 100
	// f, _ := os.Create(fmt.Sprintf("stock-%d-%s.txt", sh.stockId, name))

subscribe:
	for {
		Logger.Printf("Stock \033[33m%d\033[0m\tRecvLoop (\033[33m%s\033[0m) Buffer %d/%d\n", sh.stockId, name, len(data), bufferSize)
		for len(data) > limit {
			time.Sleep(waitTimeout)
		}

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
	blr.Load("")

	caches := make([]*common.BLOrderComp, 0)
	sources := make([]chan *common.BLOrderComp, 0)
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

	lastId := int32(0)
	next := func() *common.BLOrderComp {
		for k, v := range caches {
			if v != nil && v.OrderId == lastId+1 {
				caches[k] = nil
				lastId++
				return v
			}
		}
		return nil
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
			Logger.Fatalf("Stock \033[33m%d\033[0m\tMergeLoop no data next = %d", sh.stockId, lastId)
		}
		chosen, recv, ok := reflect.Select(cases[:m])
		if ok {
			comp := recv.Interface().(*common.BLOrderComp)
			if comp == nil {
				Logger.Fatalln("FUCKED")
			}
			caches[locs[chosen]] = comp
		} else {
			remove(locs[chosen])
		}
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
			sh.store.source <- trades
			// for _, trade := range trades {
			// fmt.Fprintf(g, "%d %d %d %f %d\n", trade.StkCode, trade.AskId, trade.BidId, trade.Price, trade.Volume)
			// }
		}
	}

	sh.store.Close()
	Logger.Printf("Stock \033[33m%d\033[0m\tMergeLoop done\n", sh.stockId)
	sh.hub.wg.Done()
}

func (sh *StockHandler) Peek(id int32) *common.BLTradeComp {
	Logger.Printf("Stock \033[33m%d\033[0m\tPeek \033[33m%d\033[0m\n", sh.stockId, id)
	comp, _ := sh.store.TryGet(id)
	return comp
}

func (sh *StockHandler) Start() {
	cacheSize := Config.TradeStoreSize
	sh.store = CreateTradeStore(cacheSize)
	go sh.store.HandleLoop()
	for _, master := range Config.Masters {
		sh.subscribes[master.Name] = make(chan *StockSubscribeRequest)
		sh.datas[master.Name] = make(chan *common.BLOrderComp, 10000000)

		sh.hub.wg.Add(1)
		go sh.SendLoop(master.Name)
		go sh.RecvLoop(master.Name)
	}
	sh.hub.wg.Add(1)
	go sh.MergeLoop()
}
