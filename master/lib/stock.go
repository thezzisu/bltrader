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
	ordPtr  int32
}

func (si *StockInfo) String() string {
	return fmt.Sprintf(
		"StockInfo {\n\tstock = %d\n}",
		si.StockId,
	)
}

func (si *StockInfo) Slide() {
	//
}

func (si *StockInfo) Seek(etag int32) {
	si.ordPtr = etag
}

func (si *StockInfo) Next() *common.BLOrder {
	if si.ordPtr == 50_000_000 {
		return nil
	}
	si.ordPtr++
	return &common.BLOrder{
		StkCode: si.StockId,
		OrderId: si.ordPtr,
	}
}

func CreateStockInfo(stockId int32) *StockInfo {
	return &StockInfo{
		StockId: stockId,
		ordPtr:  0,
	}
}

type StockOrderDep struct {
	arg int32
	val int32
	ch  chan struct{}
}

type StockSubscribeRequest struct {
	etag int32
	ch   chan *common.BLOrderDTO
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
	ch := make(chan *common.BLOrderDTO, 128)
	select {
	case sh.subscribes <- &StockSubscribeRequest{etag: etag, ch: ch}:
		return ch

	// 100ms timeout
	case <-time.After(time.Millisecond * 100):
		close(ch)
		return nil
	}
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
	ch := make(chan *common.BLOrderDTO)
	info := CreateStockInfo(sh.stockId)

	replace := func(req *StockSubscribeRequest, eager bool) {
		Logger.Printf("StockHandler[%d].SendLoop: slave subscribed since %d\n", sh.stockId, req.etag)
		if !eager {
			close(ch)
		}
		ch = req.ch
		info.Seek(req.etag)
	}

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

		if dep, ok := sh.deps[order.OrderId]; ok {
			<-dep.ch
			if dep.val > dep.arg {
				continue
			}
		}

		dto := new(common.BLOrderDTO)
		common.MarshalOrderDTO(order, dto)

		select {
		case req := <-sh.subscribes:
			replace(req, false)
		case ch <- dto:
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
			case <-time.After(timeout):
				Logger.Printf("StockHandler[%d].RecvLoop timeout\n", sh.stockId)
				continue subscribe
			}
		}
	}
	Logger.Printf("StockHandler[%d].RecvLoop done\n", sh.stockId)
	writer.Flush()
	f.Close()
	sh.hub.wg.Done()
}

func (sh *StockHandler) Start() {
	sh.hub.wg.Add(1)
	go sh.SendLoop()
	go sh.RecvLoop()
}
