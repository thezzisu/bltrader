package lib

import (
	"encoding/binary"
	"io"

	"github.com/thezzisu/bltrader/common"
)

var (
	HEADER_OK  []byte
	HEADER_ERR []byte
)

func init() {
	HEADER_OK = make([]byte, 5)
	binary.LittleEndian.PutUint32(HEADER_OK, Config.Magic)
	HEADER_OK[4] = common.RPC_STATUS_OK

	HEADER_ERR = make([]byte, 5)
	binary.LittleEndian.PutUint32(HEADER_ERR, Config.Magic)
	HEADER_ERR[4] = common.RPC_STATUS_ERROR
}

type Hub struct {
	stocks []*StockHandler
	api    *API
	rpc    *RPC

	command chan *IPCRequest
}

func (h *Hub) GetCommandChan() chan<- *IPCRequest {
	return h.command
}

func (h *Hub) rpcEcho(conn io.ReadWriteCloser) {
	var n uint32
	err := binary.Read(conn, binary.LittleEndian, &n)
	if err != nil {
		Logger.Println("Hub.rpcEcho:", err)
		return
	}
	Logger.Printf("Echo size = %d\n", n)
	buf := make([]byte, n)
	_, err = io.ReadFull(conn, buf)
	if err != nil {
		Logger.Println("Hub.rpcEcho:", err)
		return
	}
	Logger.Printf("Echo msg = %s\n", string(buf))
	// Response
	conn.Write(HEADER_OK)
	binary.Write(conn, binary.LittleEndian, n)
	conn.Write(buf)
}

func (h *Hub) HandleConn(conn io.ReadWriteCloser) {
	header := make([]byte, 5)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		Logger.Println("Hub.HandleConn: read header failed")
		return
	}
	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic != Config.Magic {
		Logger.Println("Hub.HandleConn: bad request")
		conn.Close()
		return
	}
	method := uint(header[4])
	switch method {
	case common.RPC_ECHO:
		h.rpcEcho(conn)
	default:
		Logger.Println("Hub.HandleConn: unknown method")
		header[4] = common.RPC_STATUS_ERROR
		conn.Write(header)
	}
	conn.Close()
}

func (h *Hub) exit() {
	h.api.Close()
	h.rpc.Close()
	for _, stock := range h.stocks {
		stock.Close()
	}
}

func (h *Hub) MainLoop() {
	for _, stock := range h.stocks {
		go stock.RecvLoop()
		go stock.SendLoop()
	}
	go h.api.MainLoop()
	go h.rpc.MainLoop()
	for {
		command := <-h.command
		switch command.Method {
		case IPC_EXIT:
			h.exit()
			return
		case IPC_LOG:
			Logger.Println("Hub.MainLoop: Hello!")
		default:
			Logger.Fatalln("Hub.MainLoop: unknown command")
		}
	}
}

func CreateHub() *Hub {
	hub := new(Hub)
	hub.stocks = make([]*StockHandler, 0)
	for i := 0; i < 10; i++ {
		hub.stocks = append(hub.stocks, CreateStockHandler(hub, int32(i)))
	}
	for _, stock := range hub.stocks {
		stock.InitDeps()
	}

	hub.command = make(chan *IPCRequest)

	hub.api = CreateAPI(hub)
	hub.rpc = CreateRPC(hub)
	return hub
}
