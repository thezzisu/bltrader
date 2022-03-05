package lib

import "time"

type Hub struct {
	rpc *RPC
}

func (h *Hub) MainLoop() {
	go h.rpc.MainLoop()
	time.Sleep(time.Second)
	conn, err := h.rpc.Dial()
	if err != nil {
		Logger.Fatalln(err)
	}
	conn.Write([]byte("Hello!"))
	conn.Close()
}

func CreateHub() *Hub {
	hub := new(Hub)
	hub.rpc = CreateRPC()
	return hub
}
