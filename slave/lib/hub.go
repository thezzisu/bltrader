package lib

import "time"

type Hub struct {
	rpc *RPC
}

func (h *Hub) MainLoop() {
	go h.rpc.MainLoop()
	time.Sleep(time.Second)
	msg, _ := h.rpc.RpcEcho("fuck ccf")
	println(msg)
}

func CreateHub() *Hub {
	hub := new(Hub)
	hub.rpc = CreateRPC()
	return hub
}
