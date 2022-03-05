package lib

import (
	"fmt"
	"os"
	"path"

	"github.com/thezzisu/bltrader/common"
)

type StockInfo struct {
	StockId int32

	Hooks []common.BLHook

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

func (si *StockInfo) Query(orderL int32, orderR int32) []common.BLOrder {
	return nil
}

func CreateStockInfo(stockId int32) *StockInfo {
	if ChunkCount == 1 {
		// Since we only have one chunk, just load it as cacheR
		return &StockInfo{
			StockId: stockId,
			Hooks:   LoadHooks(stockId),
			cacheL:  make([]common.BLOrder, 0),
			chunkL:  0,
			cacheR:  LoadOrderChunk(stockId, 0),
			chunkR:  0,
		}
	} else {
		return &StockInfo{
			StockId: stockId,
			Hooks:   LoadHooks(stockId),
			cacheL:  LoadOrderChunk(stockId, 0),
			chunkL:  0,
			cacheR:  LoadOrderChunk(stockId, 1),
			chunkR:  1,
		}
	}
}

type IPCRequest struct {
	Cb chan<- interface{}

	// 0 - exit
	// 1 - query
	Method  int
	Payload interface{}
}

type StockHandler struct {
	StockId int32
	info    *StockInfo

	dataDir string

	command chan IPCRequest
}

type StockHandlerQueryArgs struct {
	l, r int32
}

func (sh *StockHandler) query(l, r int32) []common.BLOrder {
	return nil
}

func (sh *StockHandler) GetCommandChan() chan<- IPCRequest {
	return sh.command
}

func (sh *StockHandler) MainLoop() {
	for {
		command := <-sh.command
		switch command.Method {
		case 0:
			return
		case 1:
			payload := command.Payload.(StockHandlerQueryArgs)
			result := sh.query(payload.l, payload.r)
			command.Cb <- result
		}
	}
}

func CreateStockHandler(stockId int32) *StockHandler {
	dataDir := path.Join(Config.DataDir, fmt.Sprint(stockId))
	err := os.MkdirAll(dataDir, 0700)
	if err != nil {
		Logger.Fatalln(err)
	}
	info := CreateStockInfo(stockId)
	return &StockHandler{
		StockId: stockId,
		info:    info,
		dataDir: dataDir,
	}
}
