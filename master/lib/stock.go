package lib

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"time"
	"unsafe"

	"github.com/thezzisu/bltrader/common"
)

var nativeEndian binary.ByteOrder

func init() {
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

	switch buf {
	case [2]byte{0xCD, 0xAB}:
		nativeEndian = binary.LittleEndian
	case [2]byte{0xAB, 0xCD}:
		nativeEndian = binary.BigEndian
	default:
		panic("Could not determine native endianness.")
	}
}

type StockInfo struct {
	StockId int32
	ordPtr  int
	incR    bool
	cacheL  []common.BLOrder
	chunkL  int
	cacheR  []common.BLOrder
	chunkR  int
}

func CreateStockInfo(stockId int32) *StockInfo {
	if ChunkCount == 1 {
		// Since we only have one chunk, just load it as cacheR
		return &StockInfo{
			StockId: stockId,
			cacheL:  make([]common.BLOrder, 0),
			chunkL:  0,
			cacheR:  LoadOrderChunk(stockId, 0),
			chunkR:  0,
			ordPtr:  0,
			incR:    true,
		}
	} else {
		return &StockInfo{
			StockId: stockId,
			cacheL:  LoadOrderChunk(stockId, 0),
			chunkL:  0,
			cacheR:  LoadOrderChunk(stockId, 1),
			chunkR:  1,
			ordPtr:  0,
			incR:    false,
		}
	}
}

func (si *StockInfo) String() string {
	return fmt.Sprintf(
		"StockInfo {\n\tstock = %d\n\twinL = [%d, %d]\n\twinR = [%d, %d]\n}",
		si.StockId,
		si.cacheL[0].OrderId,
		si.cacheL[len(si.cacheL)-1].OrderId,
		si.cacheR[0].OrderId,
		si.cacheR[len(si.cacheR)-1].OrderId,
	)
}

func (si *StockInfo) Slide() {
	if si.chunkR < ChunkCount-1 {
		si.chunkL = si.chunkR
		si.cacheL = si.cacheR
		si.chunkR++
		si.cacheR = LoadOrderChunk(si.StockId, si.chunkR)
	} else {
		Logger.Fatalf("Stock %d\tStockInfo.Slide: no more data\n", si.StockId)
	}
}

func (si *StockInfo) Seek(etag int32) {
	if len(si.cacheL) > 0 && si.cacheL[len(si.cacheL)-1].OrderId > etag {
		si.incR = false
	}
	for si.cacheR[len(si.cacheR)-1].OrderId <= etag {
		if si.chunkR == ChunkCount-1 {
			si.ordPtr = len(si.cacheR)
			si.incR = true
			return
		}
		si.incR = true
		si.Slide()
	}
	if si.incR {
		lp, rp := 0, len(si.cacheR)-1
		for lp < rp {
			mid := (lp + rp) / 2
			if si.cacheR[mid].OrderId == etag {
				si.ordPtr = mid + 1
				return
			} else if si.cacheR[mid].OrderId < etag {
				lp = mid + 1
			} else {
				rp = mid
			}
		}
		si.ordPtr = lp
	} else {
		lp, rp := 0, len(si.cacheL)-1
		for lp < rp {
			mid := (lp + rp) / 2
			if si.cacheL[mid].OrderId == etag {
				si.ordPtr = mid + 1
				return
			} else if si.cacheL[mid].OrderId < etag {
				lp = mid + 1
			} else {
				rp = mid
			}
		}
		si.ordPtr = lp
	}
}

func (si *StockInfo) Next() *common.BLOrder {
	if !si.incR {
		if si.ordPtr == len(si.cacheL) {
			si.incR = true
			si.ordPtr = 0
			return &si.cacheR[0]
		}
		si.ordPtr++
		return &si.cacheL[si.ordPtr-1]
	}
	if si.ordPtr == len(si.cacheR) {
		if si.chunkR == ChunkCount-1 {
			return nil
		}
		si.Slide()
		si.ordPtr = 0
	}
	si.ordPtr++
	return &si.cacheR[si.ordPtr-1]
}

type StockOrderDep struct {
	arg int32
	val int32
	ch  chan struct{}
}

type StockSubscribeRequest struct {
	etag   int32
	result chan chan *common.BLOrderDTO
}

type StockHandler struct {
	hub        *Hub
	remote     *Remote
	stockId    int32
	hooks      []common.BLHook
	dataDir    string
	interested map[int32][]*StockOrderDep
	deps       map[int32]*StockOrderDep
	subscribes chan *StockSubscribeRequest
}

func CreateStockHandler(hub *Hub, stockId int32) *StockHandler {
	dataDir := Config.DataDir
	err := os.MkdirAll(dataDir, 0700)
	if err != nil {
		Logger.Fatalln(err)
	}

	sh := new(StockHandler)
	sh.hub = hub
	sh.remote = hub.remotes[StockMap[stockId]]
	sh.stockId = stockId
	sh.hooks = LoadHooks(stockId)
	sh.dataDir = dataDir
	sh.interested = make(map[int32][]*StockOrderDep)
	sh.deps = make(map[int32]*StockOrderDep)
	sh.subscribes = make(chan *StockSubscribeRequest)
	return sh
}

func (sh *StockHandler) Interest(tradeId int32, dep *StockOrderDep) {
	if _, ok := sh.interested[tradeId]; !ok {
		sh.interested[tradeId] = make([]*StockOrderDep, 0)
	}
	sh.interested[tradeId] = append(sh.interested[tradeId], dep)
}

func (sh *StockHandler) InitDeps() {
	for _, hook := range sh.hooks {
		dep := StockOrderDep{
			arg: hook.Arg,
			ch:  make(chan struct{}),
		}
		sh.deps[hook.SelfOrderId] = &dep
		sh.hub.stocks[hook.TargetStkCode].Interest(hook.TargetTradeIdx, &dep)
	}
}

func (sh *StockHandler) Subscribe(etag int32) <-chan *common.BLOrderDTO {
	result := make(chan chan *common.BLOrderDTO, 128)
	sh.subscribes <- &StockSubscribeRequest{etag, result}
	ch := <-result
	return ch
}

func (sh *StockHandler) TradeHook(tradeId int32, trade *common.BLTrade) {
	if deps, ok := sh.interested[tradeId]; ok {
		fmt.Println(tradeId)
		fmt.Println(trade)
		for _, dep := range deps {
			dep.val = trade.Volume
			close(dep.ch)
		}
	}
}

func (sh *StockHandler) SendLoop() {
	info := CreateStockInfo(sh.stockId)
	var ch chan *common.BLOrderDTO
	var lastTag int32
	f, _ := os.Create(fmt.Sprintf("stock-%d.txt", sh.stockId))

	replace := func(req *StockSubscribeRequest, eager bool) {
		Logger.Printf("Stock %d\tslave subscribed since %d\n", sh.stockId, req.etag)
		if !eager {
			close(ch)
		}
		ch = make(chan *common.BLOrderDTO)
		req.result <- ch
		info.Seek(req.etag)
		fmt.Fprintf(f, "Seek at %d\n", req.etag)
		lastTag = req.etag
	}

	replace(<-sh.subscribes, true)

subscribeLoop:
	for {
		order := info.Next()
		if order == nil {
			// Send finished
			// Write EOF to remote
			dto := new(common.BLOrderDTO)
			common.MarshalOrderDTO(&common.BLOrder{
				StkCode: sh.stockId,
				OrderId: -1,
			}, dto)

			select {
			// New subscriber
			case req := <-sh.subscribes:
				replace(req, false)

			// EOF sent, waiting for new subscriber
			case ch <- dto:
				close(ch)
				req := <-sh.subscribes
				replace(req, true)
			}
			continue
		}

		if dep, ok := sh.deps[order.OrderId]; ok && order.Volume > 0 {
		depLoop:
			for {
				select {
				case <-dep.ch:
					if dep.val > dep.arg {
						order.Volume = 0
					}
					break depLoop

				case req := <-sh.subscribes:
					if req.etag == lastTag {
						Logger.Println("BLOCK\t\t", sh.stockId, order.OrderId)
						req.result <- nil
					} else {
						replace(req, false)
						continue subscribeLoop
					}
				}
			}
		}

		dto := new(common.BLOrderDTO)
		common.MarshalOrderDTO(order, dto)

		select {
		case req := <-sh.subscribes:
			replace(req, false)
		case ch <- dto:
			lastTag = order.OrderId
			fmt.Fprintf(f, "%d\n", order.OrderId)
		}
	}
}

func (sh *StockHandler) RecvLoop() {
	f, err := os.Create(path.Join(sh.dataDir, fmt.Sprintf("trade%d", sh.stockId+1)))
	if err != nil {
		Logger.Fatalf("Stock %d\tRecvLoop %v\n", sh.stockId, err)
	}
	err = f.Chmod(0600)
	if err != nil {
		Logger.Fatalf("Stock %d\tRecvLoop %v\n", sh.stockId, err)
	}
	timeout := time.Millisecond * time.Duration(Config.StockRecvTimeoutMs)
	writer := bufio.NewWriter(f)
	lastId := int32(0)
subscribe:
	for {
		ch := sh.remote.Subscribe(sh.stockId, lastId)
		if ch == nil {
			continue
		}
		for {
			timer := time.NewTimer(timeout)
			select {
			case trade, ok := <-ch:
				if !timer.Stop() {
					<-timer.C
				}
				if !ok {
					continue subscribe
				}
				if trade.AskId == -1 {
					break subscribe
				}
				lastId++
				sh.TradeHook(lastId, trade)
				binary.Write(writer, nativeEndian, sh.stockId+1)
				binary.Write(writer, nativeEndian, trade.BidId)
				binary.Write(writer, nativeEndian, trade.AskId)
				binary.Write(writer, nativeEndian, trade.Price)
				binary.Write(writer, nativeEndian, trade.Volume)
			case <-timer.C:
				Logger.Printf("Stock %d\tRecvLoop timeout\n", sh.stockId)
				continue subscribe
			}
		}
	}
	Logger.Printf("Stock %d\tRecvLoop done\n", sh.stockId)
	writer.Flush()
	f.Close()
	sh.hub.wg.Done()
}

func (sh *StockHandler) Start() {
	sh.hub.wg.Add(1)
	go sh.SendLoop()
	go sh.RecvLoop()
}
