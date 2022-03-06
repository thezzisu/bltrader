package lib

import "time"

type Hub struct {
	stocks []*StockHandler
	rpcs   map[string]*RPC
}

func (h *Hub) MainLoop() {
	go h.rpc.MainLoop()
	time.Sleep(time.Second)
	msg, _ := h.rpc.RpcEcho("fuck ccf")
	println(msg)
}

func CreateHub() *Hub {
	hub := new(Hub)
	hub.rpcs = make(map[string]*RPC)
	// for _, slave := range Config.sla
	return hub
}
