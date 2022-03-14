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
	for _, slave := range Config.Slaves {
		hub.remotes[slave.Name] = CreateRemote(hub, slave.Name)
	}

	hub.stocks = make(map[int32]*StockHandler)
	for stock := range StockMap {
		hub.stocks[stock] = CreateStockHandler(hub, stock)
	}
	for _, stock := range hub.stocks {
		stock.InitDeps()
	}

	return hub
}

func (h *Hub) Start() {
	Logger.Println("\033[34m  ___ _  _____            _")
	Logger.Println(" | _ ) ||_   _| _ __ _ __| |___ _ _")
	Logger.Println(" | _ \\ |__| || '_/ _` / _` / -_) '_|")
	Logger.Println(" |___/____|_||_| \\__,_\\__,_\\___|_|")
	Logger.Println("  __  __         _")
	Logger.Println(" |  \\/  |__ _ __| |_ ___ _ _")
	Logger.Println(" | |\\/| / _` (_-<  _/ -_) '_|")
	Logger.Println(" |_|  |_\\__,_/__/\\__\\___|_|\033[0m")

	for _, stock := range h.stocks {
		stock.Start()
	}
	for _, remote := range h.remotes {
		remote.Start()
	}
	h.wg.Wait()
}
