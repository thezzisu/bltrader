package lib

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path"

	"github.com/thezzisu/bltrader/common"
)

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
	hub *Hub

	StockId int32
	info    *StockInfo

	dataDir string

	interested map[int32][]chan int32
	deps       map[int32]StockOrderDep

	incomingConn chan net.Conn
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

func (sh *StockHandler) Close() {
	//
}

func (sh *StockHandler) Handle(conn net.Conn) {
	sh.incomingConn <- conn
}

func (sh *StockHandler) SendLoop() {
	for {
		conn := <-sh.incomingConn

		for {
			var etag int32
			err := binary.Read(conn, binary.LittleEndian, &etag)
			if err != nil {
				break
			}
		loop:
			for {
				select {
				case newConn := <-sh.incomingConn:
					conn.Close()
					conn = newConn
					break loop
				default:
				}
				order := sh.info.cacheL[0]
				binary.Write(conn, binary.LittleEndian, order.OrderId)
				binary.Write(conn, binary.LittleEndian, order.Direction)
				binary.Write(conn, binary.LittleEndian, order.Type)
				binary.Write(conn, binary.LittleEndian, order.Price)
				binary.Write(conn, binary.LittleEndian, order.Volume)
			}
		}
	}
}

func (sh *StockHandler) RecvLoop() {
	Logger.Printf("StockHandler %d: interested %d, dep %d\n", sh.StockId, len(sh.interested), len(sh.deps))
	for {
		//
	}
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

func CreateStockHandler(hub *Hub, stockId int32) *StockHandler {
	dataDir := path.Join(Config.DataDir, fmt.Sprint(stockId))
	err := os.MkdirAll(dataDir, 0700)
	if err != nil {
		Logger.Fatalln(err)
	}

	sh := new(StockHandler)
	sh.hub = hub
	sh.StockId = stockId
	sh.info = CreateStockInfo(stockId)
	sh.dataDir = dataDir
	sh.interested = make(map[int32][]chan int32)
	sh.deps = make(map[int32]StockOrderDep)
	sh.incomingConn = make(chan net.Conn)

	return sh
}
