package lib

import (
	"net"
	"sync"
	"time"

	"github.com/thezzisu/bltrader/common"
	"github.com/thezzisu/bltrader/compress"
	"github.com/thezzisu/bltrader/smux"
)

var smuxConfig = &smux.Config{
	Version:           1,
	KeepAliveInterval: 1 * time.Second,
	KeepAliveTimeout:  3 * time.Second,
	MaxFrameSize:      32768,
	MaxReceiveBuffer:  4194304,
	MaxStreamBuffer:   65536,
}

type RPCEndpoint struct {
	rpc *RPC
	hub *Hub

	listener *net.TCPListener
	conn     net.Conn
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
		Logger.Println("RPCEndpoint.MainLoop:ResolveTCPAddr", err)
		return
	}
	e.listener, err = net.ListenTCP("tcp", addr)
	if err != nil {
		Logger.Println("RPCEndpoint.MainLoop:ListenTCP", err)
		return
	}
	defer e.listener.Close()

	Logger.Printf("Endpoint listening on %s", e.Pair.MasterAddr)

	for !e.IsClosed() {
		e.conn, err = e.listener.AcceptTCP()
		if err != nil {
			Logger.Println("RPCEndpoint.MainLoop:AcceptTCP", err)
			continue
		}
		if Config.Compress {
			e.conn = compress.NewCompStream(e.conn)
		}
		Logger.Printf("Endpoint accepted connection from %s", e.conn.RemoteAddr().String())
		e.sess, err = smux.Server(e.conn, smuxConfig)
		if err != nil {
			Logger.Println("RPCEndpoint.MainLoop:smux.Server", err)
			continue
		}
		for !e.sess.IsClosed() {
			stream, err := e.sess.AcceptStream()
			if err != nil {
				Logger.Println("RPCEndpoint.MainLoop:AcceptStream", err)
				break
			}
			go e.hub.HandleConn(stream)
		}
		e.sess.Close()
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

	endpoints []*RPCEndpoint

	die     chan struct{}
	dieOnce sync.Once
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
	r.dieOnce.Do(func() {
		for _, endpoint := range r.endpoints {
			endpoint.Close()
		}
		r.pairManager.Close()
		close(r.die)
	})
}

func (r *RPC) IsClosed() bool {
	select {
	case <-r.die:
		return true
	default:
		return false
	}
}

func (r *RPC) MainLoop() {
	r.Reload()
	r.pairManager.WatchForChange()
	reload := r.pairManager.GetEventChan()
	for {
		select {
		case <-reload:
			r.Reload()
		case <-r.die:
			return
		}
	}
}

func CreateRPC(hub *Hub) *RPC {
	rpc := new(RPC)
	rpc.hub = hub
	rpc.pairManager = common.CreateRPCPairManager()
	rpc.endpoints = make([]*RPCEndpoint, 0)
	rpc.die = make(chan struct{})
	return rpc
}
