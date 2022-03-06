package lib

type Hub struct {
	rpcs   map[string]*RPC
	stocks map[int32]*StockHandler

	command chan *IPCRequest
}

func CreateHub() *Hub {
	hub := new(Hub)

	hub.rpcs = make(map[string]*RPC)
	for _, slave := range Config.Slaves {
		hub.rpcs[slave.Name] = CreateRPC(hub, slave.Name)
	}

	hub.stocks = make(map[int32]*StockHandler)
	for stock := range StockMap {
		hub.stocks[stock] = CreateStockHandler(hub, stock)
	}
	for _, stock := range hub.stocks {
		stock.InitDeps()
	}

	hub.command = make(chan *IPCRequest)
	return hub
}

func (h *Hub) GetCommandChan() chan<- *IPCRequest {
	return h.command
}

func (h *Hub) MainLoop() {
	for _, stock := range h.stocks {
		stock.Start()
	}
	for _, rpc := range h.rpcs {
		rpc.Start()
	}
	for {
		command := <-h.command
		switch command.Method {
		case IPC_LOG:
			Logger.Println("Hub.MainLoop: Hello!")
		default:
			Logger.Fatalln("Hub.MainLoop: unknown command")
		}
	}
}
