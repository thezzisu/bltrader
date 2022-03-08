package lib

import (
	"encoding/binary"
	"fmt"
	"net"
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

// TODO implement Close()
type StockHandler struct {
	hub    *Hub
	remote *Remote

	stockId int32
	info    *StockInfo

	dataDir string

	interested map[int32][]chan int32
	deps       map[int32]StockOrderDep

	incomingConn chan net.Conn
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
	sh.incomingConn = make(chan net.Conn)

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

func (sh *StockHandler) Handle(conn net.Conn) {
	sh.incomingConn <- conn
}

func (sh *StockHandler) SendLoop() {
	Logger.Printf("StockHandler[%d].SendLoop started\n", sh.stockId)

	for {
		conn := <-sh.incomingConn

	connLoop:
		for {
			var etag int32
			err := binary.Read(conn, binary.LittleEndian, &etag)
			if err != nil {
				break
			}
		sessLoop:
			for {
				select {
				case newConn := <-sh.incomingConn:
					conn.Close()
					conn = newConn
					break sessLoop
				default:
				}
				// TODO
				// order := sh.info.cacheL[0]
				// err := binary.Write(conn, binary.LittleEndian, common.BLOrderDTO{
				// 	OrderId:   order.OrderId,
				// 	Direction: order.Direction,
				// 	Type:      order.Type,
				// 	Price:     order.Price,
				// 	Volume:    order.Volume,
				// })
				if err != nil {
					break connLoop
				}
			}
		}
		conn.Close()
	}
}

func (sh *StockHandler) RecvLoop() {
	// TODO
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

func (sh *StockHandler) Start() {
	sh.hub.wg.Add(1)
	go sh.SendLoop()
	go sh.RecvLoop()
}
