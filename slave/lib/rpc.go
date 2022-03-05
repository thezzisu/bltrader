package lib

import (
	"encoding/binary"
	"io"
	"math/rand"
	"net"
	"sync"

	"github.com/thezzisu/bltrader/common"
	"github.com/thezzisu/bltrader/compress"
	"github.com/thezzisu/bltrader/smux"
)

type RPCEndpoint struct {
	conn net.Conn
	sess *smux.Session

	die     chan struct{}
	dieOnce sync.Once
	err     chan struct{}

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
	laddr, err := net.ResolveTCPAddr("tcp", e.Pair.SlaveAddr)
	if err != nil {
		Logger.Println(err)
		return
	}
	raddr, err := net.ResolveTCPAddr("tcp", e.Pair.MasterAddr)
	if err != nil {
		Logger.Println(err)
		return
	}
	for !e.IsClosed() {
		e.conn, err = net.DialTCP("tcp", laddr, raddr)
		if err != nil {
			Logger.Println(err)
			return
		}
		if Config.Compress {
			e.conn = compress.NewCompStream(e.conn)
		}
		e.sess, err = smux.Client(e.conn, nil)
		if err != nil {
			Logger.Println(err)
			return
		}
		select {
		case <-e.err:
		case <-e.die:
		}
	}
}

func (e *RPCEndpoint) Dial() (io.ReadWriteCloser, error) {
	stream, err := e.sess.Open()
	if err != nil {
		e.err <- struct{}{}
		return nil, err
	}
	return stream, nil
}

func createRPCEndpoint(r *RPC, pair common.RPCPair) *RPCEndpoint {
	e := new(RPCEndpoint)
	e.die = make(chan struct{})
	e.err = make(chan struct{})
	e.Pair = pair
	return e
}

type RPC struct {
	pairManager *common.RPCPairManager
	endpoints   []*RPCEndpoint
	die         chan struct{}
	dieOnce     sync.Once
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

func (r *RPC) Dial() (io.ReadWriteCloser, error) {
	n := len(r.endpoints)
	if n == 0 {
		return nil, ErrAgain
	}
	endpoint := r.endpoints[rand.Intn(n)]
	return endpoint.Dial()
}

func (r *RPC) RpcEcho(msg string) (string, error) {
	conn, err := r.Dial()
	if err != nil {
		return "", err
	}

	header := make([]byte, 5)
	binary.LittleEndian.PutUint32(header, Config.Magic)
	header[4] = common.RPC_ECHO

	conn.Write(header)

	body := []byte(msg)
	length := make([]byte, 4)
	binary.LittleEndian.PutUint32(length, uint32(len(body)))

	conn.Write(length)
	conn.Write(body)

	io.ReadFull(conn, header)
	if err != nil {
		return "", err
	}

	if header[4] != common.RPC_STATUS_OK {
		return "", common.ErrRPC
	}

	io.ReadFull(conn, length)
	buf := make([]byte, binary.LittleEndian.Uint32(length))
	io.ReadFull(conn, buf)

	return string(buf), nil
}

func CreateRPC() *RPC {
	rpc := new(RPC)
	rpc.pairManager = common.CreateRPCPairManager()
	rpc.endpoints = make([]*RPCEndpoint, 0)
	rpc.die = make(chan struct{})
	return rpc
}
