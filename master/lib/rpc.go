package lib

import (
	"net"
	"sync"

	"github.com/thezzisu/bltrader/common"
	"github.com/thezzisu/bltrader/smux"
)

type RPCEndpoint struct {
	rpc *RPC
	hub *Hub

	listener *net.TCPListener
	conn     *net.TCPConn
	sess     *smux.Session

	die     chan struct{}
	dieOnce sync.Once

	Pair common.RPCPair
}

func (e *RPCEndpoint) Close() {
	e.dieOnce.Do(func() {
		if e.sess != nil {
			e.sess.Close()
		}
		if e.conn != nil {
			e.conn.Close()
		}
		if e.listener != nil {
			e.listener.Close()
		}
		close(e.die)
	})
}

func (e *RPCEndpoint) IsClosed() bool {
	select {
	case <-e.die:
		return true
	default:
		return false
	}
}

func (e *RPCEndpoint) MainLoop() {
	addr, err := net.ResolveTCPAddr("tcp", e.Pair.MasterAddr)
	if err != nil {
		Logger.Println(err)
		return
	}
	e.listener, err = net.ListenTCP("tcp", addr)
	if err != nil {
		Logger.Println(err)
		return
	}
	defer e.listener.Close()

	Logger.Printf("Endpoint listening on %s", e.Pair.MasterAddr)

	for !e.IsClosed() {
		e.conn, err = e.listener.AcceptTCP()
		if err != nil {
			Logger.Println(err)
			continue
		}
		Logger.Printf("Endpoint accepted connection from %s", e.conn.RemoteAddr().String())
		e.sess, err = smux.Server(e.conn, nil)
		if err != nil {
			Logger.Println(err)
			continue
		}
		for !e.sess.IsClosed() {
			stream, err := e.sess.AcceptStream()
			if err != nil {
				Logger.Println(err)
				continue
			}
			go e.hub.HandleConn(stream)
		}
		e.conn.Close()
	}
}

func createRPCEndpoint(rpc *RPC, pair common.RPCPair) *RPCEndpoint {
	e := new(RPCEndpoint)
	e.rpc = rpc
	e.hub = rpc.hub

	e.Pair = pair

	e.die = make(chan struct{})

	return e
}

type RPC struct {
	hub         *Hub
	pairManager *common.RPCPairManager

	command   chan *IPCRequest
	endpoints []*RPCEndpoint
}

func (r *RPC) GetCommandChan() chan<- *IPCRequest {
	return r.command
}

func (r *RPC) Reload() {
	pairs := r.pairManager.GetPairs()

	n := len(r.endpoints)
	m := len(pairs)
	if n > m {
		for i := len(pairs); i < len(r.endpoints); i++ {
			Logger.Printf("RPC.Reload: closing endpoint %s <-> %s", r.endpoints[i].Pair.MasterAddr, r.endpoints[i].Pair.SlaveAddr)
			r.endpoints[i].Close()
		}
		r.endpoints = r.endpoints[:m]
		n = m
	}
	for i := 0; i < n; i++ {
		if pairs[i].MasterAddr != r.endpoints[i].Pair.MasterAddr || pairs[i].SlaveAddr != r.endpoints[i].Pair.SlaveAddr {
			Logger.Printf("RPC.Reload: closing endpoint %s <-> %s", r.endpoints[i].Pair.MasterAddr, r.endpoints[i].Pair.SlaveAddr)
			r.endpoints[i].Close()
			r.endpoints[i] = createRPCEndpoint(r, pairs[i])
			go r.endpoints[i].MainLoop()
			Logger.Printf("RPC.Reload: new endpoint %s <-> %s", r.endpoints[i].Pair.MasterAddr, r.endpoints[i].Pair.SlaveAddr)
		}
	}
	for i := len(r.endpoints); i < len(pairs); i++ {
		endpoint := createRPCEndpoint(r, pairs[i])
		r.endpoints = append(r.endpoints, endpoint)
		go endpoint.MainLoop()
		Logger.Printf("RPC.Reload: new endpoint %s <-> %s", endpoint.Pair.MasterAddr, endpoint.Pair.SlaveAddr)
	}
}

func (r *RPC) Close() {
	r.command <- &IPCRequest{
		Method: IPC_EXIT,
	}
}

func (r *RPC) exit() {
	for _, endpoint := range r.endpoints {
		endpoint.Close()
	}
	r.pairManager.Close()
	close(r.command)
}

func (r *RPC) MainLoop() {
	r.Reload()
	r.pairManager.WatchForChange()
	reload := r.pairManager.GetEventChan()
	for {
		select {
		case command := <-r.command:
			switch command.Method {
			case IPC_EXIT:
				r.exit()
				return
			case IPC_LOG:
				Logger.Println("RPC.MainLoop: Hello!")
			default:
				Logger.Fatalln("RPC.MainLoop: unknown command")
			}
		case <-reload:
			r.Reload()
		}
	}
}

func CreateRPC(hub *Hub) *RPC {
	return &RPC{
		hub:         hub,
		pairManager: common.CreateRPCPairManager(),
		command:     make(chan *IPCRequest),
		endpoints:   make([]*RPCEndpoint, 0),
	}
}
