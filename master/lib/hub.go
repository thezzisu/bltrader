package lib

import "io"

type Hub struct {
	stocks []*StockHandler
	api    *API
	rpc    *RPC

	command chan *IPCRequest
}

func (h *Hub) GetCommandChan() chan<- *IPCRequest {
	return h.command
}

func (h *Hub) HandleConn(conn io.ReadWriteCloser) {
	buf := make([]byte, 1024)
	n, _ := conn.Read(buf)
	Logger.Println(string(buf[:n]))
}

func (h *Hub) exit() {
	h.api.Close()
	h.rpc.Close()
	for _, stock := range h.stocks {
		stock.Close()
	}
}

func (h *Hub) MainLoop() {
	for i := 0; i < 10; i++ {
		go h.stocks[i].MainLoop()
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
		hub.stocks = append(hub.stocks, CreateStockHandler(int32(i)))
	}

	for i := 0; i < 10; i++ {
		for _, hook := range hub.stocks[i].Info.Hooks {
			hub.stocks[hook.TargetStkCode].Interest(hook.TargetTradeIdx)
		}
	}

	hub.command = make(chan *IPCRequest)

	hub.api = CreateAPI(hub)
	hub.rpc = CreateRPC(hub)
	return hub
}
