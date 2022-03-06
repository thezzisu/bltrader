package lib

import (
	"fmt"
	"net"
	"os"
	"path"
)

// TODO implement Close()
type StockHandler struct {
}

func (sh *StockHandler) Interest(tradeId int32) <-chan int32 {
	// TODO
}

func (sh *StockHandler) Close() {
	//
}

func (sh *StockHandler) Handle(conn net.Conn) {
	// TODO
}

func (sh *StockHandler) SendLoop() {
	// TODO
}

func (sh *StockHandler) RecvLoop() {
	// TODO
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
