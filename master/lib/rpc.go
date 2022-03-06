package lib

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path"
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

	die     chan struct{}
	dieOnce sync.Once

	Pair common.RPCPair

	incomingConn chan net.Conn
	outgoingConn chan net.Conn
	dialRequest  chan struct{}
}

func createRPCEndpoint(rpc *RPC, pair common.RPCPair) *RPCEndpoint {
	e := new(RPCEndpoint)
	e.rpc = rpc
	e.hub = rpc.hub
	e.Pair = pair
	e.die = make(chan struct{})
	e.incomingConn = make(chan net.Conn)
	e.outgoingConn = make(chan net.Conn)
	e.dialRequest = make(chan struct{})

	return e
}

func (e *RPCEndpoint) Close() {
	e.dieOnce.Do(func() {
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

func (e *RPCEndpoint) handleConn(conn net.Conn) {
	var stockId int32
	err := binary.Read(conn, binary.LittleEndian, &stockId)
	if err != nil {
		conn.Close()
	}
	e.hub.stocks[stockId].Handle(conn)
}

func (e *RPCEndpoint) Dial() (net.Conn, error) {
	e.dialRequest <- struct{}{}
	return <-e.outgoingConn, nil
}

func (e *RPCEndpoint) AcceptLoop() {
	for !e.IsClosed() {
		conn := <-e.incomingConn

	connLoop:
		for !e.IsClosed() {
			sess, err := smux.Server(conn, smuxConfig)
			if err != nil {
				break
			}
		sessLoop:
			for {
				select {
				case newConn := <-e.incomingConn:
					conn.Close()
					conn = newConn
					break sessLoop

				case <-e.dialRequest:
					stream, err := sess.OpenStream()
					if err != nil {
						sess.Close()
						break connLoop
					}
					e.outgoingConn <- stream

				case <-e.die:
					sess.Close()
					break connLoop

				default:
				}
				stream, err := sess.AcceptStream()
				if err != nil {
					sess.Close()
					break connLoop
				}
				go e.handleConn(stream)
			}

			sess.Close()
		}

		conn.Close()
	}
}

func (e *RPCEndpoint) ListenLoop() {
	addr, err := net.ResolveTCPAddr("tcp", e.Pair.MasterAddr)
	if err != nil {
		Logger.Println("RPCEndpoint.MainLoop:ResolveTCPAddr", err)
		return
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		Logger.Println("RPCEndpoint.MainLoop:ListenTCP", err)
		return
	}
	defer listener.Close()

	Logger.Printf("Endpoint listening on %s", e.Pair.MasterAddr)

	for !e.IsClosed() {
		conn, err := listener.AcceptTCP()
		if err != nil {
			Logger.Println("RPCEndpoint.MainLoop:AcceptTCP", err)
			continue
		}
		var magic uint32
		err = binary.Read(conn, binary.LittleEndian, &magic)
		if err != nil || magic != Config.Magic {
			conn.Close()
			continue
		}
		Logger.Printf("Endpoint accepted connection from %s", conn.RemoteAddr().String())
		if Config.Compress {
			e.incomingConn <- compress.NewCompStream(conn)
		} else {
			e.incomingConn <- conn
		}
	}
}

func (e *RPCEndpoint) Start() {
	go e.AcceptLoop()
	go e.ListenLoop()
}

type RPC struct {
	hub         *Hub
	pairManager *common.RPCPairManager
	endpoints   []*RPCEndpoint

	Slave string

	die     chan struct{}
	dieOnce sync.Once
}

func CreateRPC(hub *Hub, slave string) *RPC {
	configPath, _ := os.UserConfigDir()
	configPath = path.Join(configPath, "bltrader", fmt.Sprintf("rpc.%s.%s.json", Config.Name, slave))

	rpc := new(RPC)
	rpc.hub = hub
	rpc.pairManager = common.CreateRPCPairManager(configPath)
	rpc.Slave = slave
	rpc.endpoints = make([]*RPCEndpoint, 0)
	rpc.die = make(chan struct{})
	return rpc
}

func (r *RPC) Reload() {
	pairs := r.pairManager.GetPairs()

	n := len(r.endpoints)
	m := len(pairs)
	if n > m {
		for i := len(pairs); i < len(r.endpoints); i++ {
			Logger.Printf("RPC[%s].Reload: closing endpoint %s <-> %s", r.Slave, r.endpoints[i].Pair.MasterAddr, r.endpoints[i].Pair.SlaveAddr)
			r.endpoints[i].Close()
		}
		r.endpoints = r.endpoints[:m]
		n = m
	}
	for i := 0; i < n; i++ {
		if pairs[i].MasterAddr != r.endpoints[i].Pair.MasterAddr || pairs[i].SlaveAddr != r.endpoints[i].Pair.SlaveAddr {
			Logger.Printf("RPC[%s].Reload: closing endpoint %s <-> %s", r.Slave, r.endpoints[i].Pair.MasterAddr, r.endpoints[i].Pair.SlaveAddr)
			r.endpoints[i].Close()
			r.endpoints[i] = createRPCEndpoint(r, pairs[i])
			r.endpoints[i].Start()
			Logger.Printf("RPC[%s].Reload: new endpoint %s <-> %s", r.Slave, r.endpoints[i].Pair.MasterAddr, r.endpoints[i].Pair.SlaveAddr)
		}
	}
	for i := len(r.endpoints); i < len(pairs); i++ {
		endpoint := createRPCEndpoint(r, pairs[i])
		r.endpoints = append(r.endpoints, endpoint)
		endpoint.Start()
		Logger.Printf("RPC[%s].Reload: new endpoint %s <-> %s", r.Slave, endpoint.Pair.MasterAddr, endpoint.Pair.SlaveAddr)
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

func (r *RPC) Start() {
	go r.MainLoop()
}

func (r *RPC) Dial() (net.Conn, error) {
	n := len(r.endpoints)
	if n == 0 {
		return nil, common.ErrNoEndpoint
	}
	endpoint := r.endpoints[rand.Intn(n)]
	return endpoint.Dial()
}
