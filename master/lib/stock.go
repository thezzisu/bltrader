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

	cacheL []common.BLOrder
	chunkL int
	cacheR []common.BLOrder
	chunkR int
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
		Logger.Fatalln("StockInfo.Slide: no more data")
	}
}

func (si *StockInfo) Seek(etag int32) {
	// TODO
}

func (si *StockInfo) Next() *common.BLOrder {
	// TODO
	return nil
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
		}
	} else {
		return &StockInfo{
			StockId: stockId,
			cacheL:  LoadOrderChunk(stockId, 0),
			chunkL:  0,
			cacheR:  LoadOrderChunk(stockId, 1),
			chunkR:  1,
		}
	}
}

type StockOrderDep struct {
	arg int32
	val int32
	ch  chan struct{}
}

type StockSubscribeRequest struct {
	etag int32
	ch   chan *common.BLOrder
}

type StockHandler struct {
	hub        *Hub
	remote     *Remote
	stockId    int32
	hooks      []common.BLHook
	dataDir    string
	interested map[int32][]*StockOrderDep
	deps       map[int32]*StockOrderDep
	subscribes chan StockSubscribeRequest
}

func CreateStockHandler(hub *Hub, stockId int32) *StockHandler {
	dataDir := path.Join(Config.DataDir, fmt.Sprint(stockId))
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

func (sh *StockHandler) Subscribe(etag int32) <-chan *common.BLOrder {
	ch := make(chan *common.BLOrder)
	sh.subscribes <- StockSubscribeRequest{etag: etag, ch: ch}
	return ch
}

func (sh *StockHandler) TradeHook(tradeId int32, trade *common.BLTrade) {
	if deps, ok := sh.interested[tradeId]; ok {
		for _, dep := range deps {
			dep.val = trade.Volume
			close(dep.ch)
		}
	}
}

func (sh *StockHandler) SendLoop() {
	ch := make(chan *common.BLOrder)
	info := CreateStockInfo(sh.stockId)

nextOrder:
	for {
		order := info.Next()
		if order == nil {
			// Send finished
			req := <-sh.subscribes
			fmt.Printf("StockHandler.SendLoop: subscribing to %d\n", req.etag)
			close(ch)
			ch = req.ch
			info.Seek(req.etag)
			continue
		}

		if dep, ok := sh.deps[order.StkCode]; ok {
			// This order is hooked
			select {
			case req := <-sh.subscribes:
				// Handle new subscriber
				fmt.Printf("StockHandler.SendLoop: subscribing to %d\n", req.etag)
				close(ch)
				ch = req.ch
				info.Seek(req.etag)
				continue nextOrder

			case <-dep.ch:
				// Depdenency is ready
				if dep.val > dep.arg {
					// Continue for next order
					continue
				}
			}
		}

		select {
		case req := <-sh.subscribes:
			// Handle new subscriber
			fmt.Printf("StockHandler.SendLoop: subscribing to %d\n", req.etag)
			close(ch)
			ch = req.ch
			info.Seek(req.etag)
		case ch <- order:
		}
	}
}

func (sh *StockHandler) RecvLoop() {
	f, err := os.Create(path.Join(sh.dataDir, fmt.Sprintf("trade%d", sh.stockId+1)))
	if err != nil {
		Logger.Fatalf("StockHandler[%d].RecvLoop %v\n", sh.stockId, err)
	}
	err = f.Chmod(0600)
	if err != nil {
		Logger.Fatalf("StockHandler[%d].RecvLoop %v\n", sh.stockId, err)
	}
	writer := bufio.NewWriter(f)
	lastId := int32(0)
subscribe:
	for {
		ch := sh.remote.Subscribe(sh.stockId, lastId)
		for {
			// TODO add configuration for this timeout
			timer := time.NewTimer(time.Second * 10)
			select {
			case trade, ok := <-ch:
				if !ok {
					break
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
				Logger.Printf("StockHandler[%d].RecvLoop timeout\n", sh.stockId)
				continue subscribe
			}
		}
	}
	writer.Flush()
	f.Close()
	sh.hub.wg.Done()
}

func (sh *StockHandler) Start() {
	sh.hub.wg.Add(1)
	go sh.SendLoop()
	go sh.RecvLoop()
}
