package lib

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path"
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

	hooks  []common.BLHook
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

func CreateStockInfo(stockId int32) *StockInfo {
	if ChunkCount == 1 {
		// Since we only have one chunk, just load it as cacheR
		return &StockInfo{
			StockId: stockId,
			hooks:   LoadHooks(stockId),
			cacheL:  make([]common.BLOrder, 0),
			chunkL:  0,
			cacheR:  LoadOrderChunk(stockId, 0),
			chunkR:  0,
		}
	} else {
		return &StockInfo{
			StockId: stockId,
			hooks:   LoadHooks(stockId),
			cacheL:  LoadOrderChunk(stockId, 0),
			chunkL:  0,
			cacheR:  LoadOrderChunk(stockId, 1),
			chunkR:  1,
		}
	}
}

type StockOrderDep struct {
	Arg int32
	Ch  <-chan int32
}

type StockHandler struct {
	hub        *Hub
	remote     *Remote
	stockId    int32
	info       *StockInfo
	dataDir    string
	interested map[int32][]chan int32
	deps       map[int32]StockOrderDep
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
	sh.info = CreateStockInfo(stockId)
	sh.dataDir = dataDir
	sh.interested = make(map[int32][]chan int32)
	sh.deps = make(map[int32]StockOrderDep)

	return sh
}

func (sh *StockHandler) Interest(tradeId int32) <-chan int32 {
	if _, ok := sh.interested[tradeId]; !ok {
		sh.interested[tradeId] = make([]chan int32, 0)
	}
	// Make chan buffered to avoid blocking
	ch := make(chan int32, 1)
	sh.interested[tradeId] = append(sh.interested[tradeId], ch)
	return ch
}

func (sh *StockHandler) InitDeps() {
	for _, hook := range sh.info.hooks {
		ch := sh.hub.stocks[hook.TargetStkCode].Interest(hook.TargetTradeIdx)
		sh.deps[hook.SelfOrderId] = StockOrderDep{
			Arg: hook.Arg,
			Ch:  ch,
		}
	}
}

func (sh *StockHandler) Subscribe(etag int32) (<-chan common.BLOrder, bool) {
	// TODO
	return nil, false
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
	for {
		ch, ok := sh.remote.Subscribe(sh.stockId, lastId)
		if !ok {
			break
		}
		for {
			trade, ok := <-ch
			if !ok {
				break
			}
			lastId++
			if _, ok := sh.interested[lastId]; ok {
				// Trade is interested
				cbs := sh.interested[lastId]
				for _, cb := range cbs {
					cb <- trade.Volume
				}
			}
			binary.Write(writer, nativeEndian, sh.stockId+1)
			binary.Write(writer, nativeEndian, trade.BidId)
			binary.Write(writer, nativeEndian, trade.AskId)
			binary.Write(writer, nativeEndian, trade.Price)
			binary.Write(writer, nativeEndian, trade.Volume)
		}
	}
	writer.Flush()
	f.Close()
	sh.hub.wg.Done()
}

func (sh *StockHandler) Start() {
	sh.hub.wg.Add(1)
	go sh.RecvLoop()
}
