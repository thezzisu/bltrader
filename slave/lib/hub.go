package lib

import "sync"

type Hub struct {
	remotes map[string]*Remote
	stocks  map[int32]*StockHandler
	wg      sync.WaitGroup
}

func CreateHub() *Hub {
	hub := new(Hub)

	hub.remotes = make(map[string]*Remote)
	for _, master := range Config.Masters {
		hub.remotes[master.Name] = CreateRemote(hub, master.Name)
	}

	hub.stocks = make(map[int32]*StockHandler)
	for _, stock := range Config.Stocks {
		hub.stocks[stock] = CreateStockHandler(hub, stock)
	}

	return hub
}

func (h *Hub) Start() {
	Logger.Println("\033[33m  ___ _  _____            _")
	Logger.Println(" | _ ) ||_   _| _ __ _ __| |___ _ _")
	Logger.Println(" | _ \\ |__| || '_/ _` / _` / -_) '_|")
	Logger.Println(" |___/____|_||_| \\__,_\\__,_\\___|_|")
	Logger.Println("  ___ _")
	Logger.Println(" / __| |__ ___ _____")
	Logger.Println(" \\__ \\ / _` \\ V / -_)")
	Logger.Println(" |___/_\\__,_|\\_/\\___|\033[0m")
	for _, stock := range h.stocks {
		stock.Start()
	}
	for _, remote := range h.remotes {
		remote.Start()
	}
	h.wg.Wait()
	Logger.Println("\033[31mRecv done. Waiting for user signal...\033[0m")
	h.wg.Add(1)
	h.wg.Wait()
}
