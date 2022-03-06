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
	e.hub.stocks[stockId].Handle(e.rpc.Master, conn)
}

func (e *RPCEndpoint) Dial() (net.Conn, error) {
	e.dialRequest <- struct{}{}
	return <-e.outgoingConn, nil
}

func (e *RPCEndpoint) DialLoop() {
	laddr, err := net.ResolveTCPAddr("tcp", e.Pair.SlaveAddr)
	if err != nil {
		Logger.Println("RPCEndpoint.DialLoop", err)
		return
	}
	raddr, err := net.ResolveTCPAddr("tcp", e.Pair.MasterAddr)
	if err != nil {
		Logger.Println("RPCEndpoint.DialLoop", err)
		return
	}

	for !e.IsClosed() {
		var conn net.Conn
		conn, err := net.DialTCP("tcp", laddr, raddr)
		if err != nil {
			Logger.Println("RPCEndpoint.DialLoop", err)

			Logger.Println(e.rpc.endpoints)
			time.Sleep(time.Second / 2)
			continue
		}
		err = binary.Write(conn, binary.LittleEndian, Config.Magic)
		if err != nil {
			Logger.Println("RPCEndpoint.DialLoop", err)
			conn.Close()
			time.Sleep(time.Second / 2)
			continue
		}
		if Config.Compress {
			conn = compress.NewCompStream(conn)
		}
		sess, err := smux.Client(conn, smuxConfig)
		if err != nil {
			Logger.Println("RPCEndpoint.DialLoop", err)
			conn.Close()
			time.Sleep(time.Second / 2)
			continue
		}
		Logger.Printf("RPCEndpoint.DialLoop: connected to %s\n", e.Pair.MasterAddr)
		acceptCh := make(chan net.Conn)
		dieCh := make(chan struct{})
		go func() {
			for {
				stream, err := sess.AcceptStream()
				if err != nil {
					common.TryClose(dieCh)
					return
				}
				acceptCh <- stream
			}
		}()
	sessLoop:
		for {
			select {
			case <-e.dialRequest:
				stream, err := sess.OpenStream()
				if err != nil {
					common.TryClose(dieCh)
					break
				}
				Logger.Println("RPCEndpoint.DialLoop: stream opened")
				e.outgoingConn <- stream

			case <-e.die:
				common.TryClose(dieCh)

			case stream := <-acceptCh:
				Logger.Println("RPCEndpoint.DialLoop: stream accepted")
				go e.handleConn(stream)

			case <-dieCh:
				Logger.Printf("RPCEndpoint.DialLoop: connection to %s closed\n", e.Pair.MasterAddr)
				sess.Close()
				close(acceptCh)
				break sessLoop
			}
		}
		conn.Close()
	}
}

func (e *RPCEndpoint) Start() {
	go e.DialLoop()
}

type RPC struct {
	hub         *Hub
	pairManager *common.RPCPairManager
	endpoints   []*RPCEndpoint

	Master string

	die     chan struct{}
	dieOnce sync.Once
}

func CreateRPC(hub *Hub, master string) *RPC {
	configPath, _ := os.UserConfigDir()
	configPath = path.Join(configPath, "bltrader", fmt.Sprintf("rpc.%s.%s.json", master, Config.Name))

	rpc := new(RPC)
	rpc.hub = hub
	rpc.pairManager = common.CreateRPCPairManager(configPath)
	rpc.Master = master
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
			r.endpoints[i].Start()
			Logger.Printf("RPC.Reload: new endpoint %s <-> %s", r.endpoints[i].Pair.MasterAddr, r.endpoints[i].Pair.SlaveAddr)
		}
	}
	for i := len(r.endpoints); i < len(pairs); i++ {
		endpoint := createRPCEndpoint(r, pairs[i])
		r.endpoints = append(r.endpoints, endpoint)
		endpoint.Start()
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
