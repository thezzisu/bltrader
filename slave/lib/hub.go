package lib

type Hub struct {
	rpcs   map[string]*RPC
	stocks map[int32]*StockHandler

	command chan *IPCRequest
}

func CreateHub() *Hub {
	hub := new(Hub)
	hub.rpcs = make(map[string]*RPC)
	for _, master := range Config.Masters {
		hub.rpcs[master.Name] = CreateRPC(hub, master.Name)
	}

	hub.stocks = make(map[int32]*StockHandler)
	for _, stock := range Config.Stocks {
		hub.stocks[stock] = CreateStockHandler(hub, stock)
	}

	return hub
}

func (h *Hub) GetCommandChan() chan<- *IPCRequest {
	return h.command
}

func (h *Hub) MainLoop() {
	Logger.Println(Config)
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
