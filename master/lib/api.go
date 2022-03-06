package lib

import "net"

type API struct {
	hub      *Hub
	listener *net.TCPListener
}

func (a *API) Close() {
	a.listener.Close()
}

func (a *API) MainLoop() {
	addr, err := net.ResolveTCPAddr("tcp", Config.Listen)
	if err != nil {
		Logger.Fatal(err)
	}

	a.listener, err = net.ListenTCP("tcp", addr)
	if err != nil {
		Logger.Fatal(err)
	}

	Logger.Printf("API listening on %s", Config.Listen)
	// TODO
}

func CreateAPI(hub *Hub) *API {
	api := new(API)
	api.hub = hub
	return api
}
