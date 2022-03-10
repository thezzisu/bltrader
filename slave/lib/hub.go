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
	for _, stock := range h.stocks {
		stock.Start()
	}
	for _, remote := range h.remotes {
		remote.Start()
	}
	h.wg.Wait()
}
