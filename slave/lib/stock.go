package lib

import (
	"encoding/binary"
	"net"
	"time"

	"github.com/thezzisu/bltrader/common"
)

// TODO implement Close()
type StockHandler struct {
	hub *Hub

	StockId      int32
	incomingConn map[string]chan net.Conn
}

func (sh *StockHandler) Handle(name string, conn net.Conn) {
	sh.incomingConn[name] <- conn
}

func (sh *StockHandler) SendLoop(name string) {
	Logger.Printf("StockHandler[%d].SendLoop(%s) started\n", sh.StockId, name)
	incomingConn := sh.incomingConn[name]

	for {
		conn := <-incomingConn

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
				case newConn := <-incomingConn:
					conn.Close()
					conn = newConn
					break sessLoop
				default:
				}
				// TODO
				dto := common.BLTradeDTO{
					BidId:  1,
					AskId:  2,
					Price:  3,
					Volume: 4,
				}
				err = binary.Write(conn, binary.LittleEndian, dto)
				time.Sleep(time.Second)
				if err != nil {
					break connLoop
				}
			}
		}
		conn.Close()
	}
}

func (sh *StockHandler) Start() {
	for _, master := range Config.Masters {
		go sh.SendLoop(master.Name)
	}
}

func CreateStockHandler(hub *Hub, stockId int32) *StockHandler {
	sh := new(StockHandler)
	sh.hub = hub
	sh.StockId = stockId

	sh.incomingConn = make(map[string]chan net.Conn)
	for _, master := range Config.Masters {
		sh.incomingConn[master.Name] = make(chan net.Conn)
	}

	return sh
}
