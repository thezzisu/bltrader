package lib

import (
	"encoding/binary"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thezzisu/bltrader/common"
)

type TransportAllocateRequest struct {
	stock     int32
	etag      int32
	handshake int32
}

type Transport struct {
	hub               *Hub
	remote            *Remote
	pair              common.RPCPair
	die               chan struct{}
	dieOnce           sync.Once
	incomingConn      chan net.Conn
	subscriptionCount int32
	allocates         chan TransportAllocateRequest
}

func CreateTransport(remote *Remote, pair common.RPCPair) *Transport {
	t := new(Transport)
	t.hub = remote.hub
	t.remote = remote
	t.pair = pair

	t.die = make(chan struct{})
	t.incomingConn = make(chan net.Conn)
	t.subscriptionCount = 0
	t.allocates = make(chan TransportAllocateRequest)

	return t
}

func (t *Transport) Close() {
	t.dieOnce.Do(func() {
		close(t.die)
	})
}

func (t *Transport) IsClosed() bool {
	select {
	case <-t.die:
		return true
	default:
		return false
	}
}

func (t *Transport) Start() {
	go t.DialLoop()
}

func (t *Transport) DialLoop() {
	raddr, err := net.ResolveTCPAddr("tcp", t.pair.MasterAddr)
	if err != nil {
		Logger.Println("Transport.DialLoop", err)
		t.Close()
		return
	}
	laddr, err := net.ResolveTCPAddr("tcp", t.pair.SlaveAddr)
	if err != nil {
		Logger.Println("Transport.DialLoop", err)
		t.Close()
		return
	}

	for !t.IsClosed() {
		// TODO add timeout
		conn, err := net.DialTCP("tcp", laddr, raddr)
		if err != nil {
			Logger.Println("Transport.DialLoop", err)
			time.Sleep(time.Second / 2)
			continue
		}
		err = binary.Write(conn, binary.LittleEndian, Config.Magic)
		if err != nil {
			Logger.Println("Transport.DialLoop", err)
			conn.Close()
			continue
		}

		err = conn.SetKeepAlive(true)
		if err != nil {
			Logger.Println("Transport.DialLoop", err)
			conn.Close()
			continue
		}
		err = conn.SetKeepAlivePeriod(time.Second * 10)
		if err != nil {
			Logger.Println("Transport.DialLoop", err)
			conn.Close()
			continue
		}

		Logger.Printf("Transport dialed connection to %s", conn.RemoteAddr().String())
		t.Handle(conn)
	}
}

func (t *Transport) Handle(conn net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { t.RecvLoop(conn); wg.Done() }()
	go func() { t.SendLoop(conn); wg.Done() }()
	wg.Wait()
}

func (t *Transport) RecvLoop(conn net.Conn) {
	for {
		var dto common.BLOrderDTO
		err := binary.Read(conn, binary.LittleEndian, &dto)
		if err != nil {
			Logger.Println("Transport.RecvLoop", err)
			conn.Close()
			return
		}
		t.remote.incoming <- &dto
	}
}

func (t *Transport) SendLoop(conn net.Conn) {
	var err error
	cases := make([]reflect.SelectCase, 2)
	cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(t.remote.command)}
	cases[1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(t.allocates)}

	for {
		chosen, recv, ok := reflect.Select(cases)
		switch chosen {
		case 0: // Handle command
			dto := recv.Interface().(*common.BLTradeDTO)
			err = binary.Write(conn, binary.LittleEndian, dto)

		case 1: // Handle allocate
			req := recv.Interface().(TransportAllocateRequest)
			ch := t.hub.stocks[req.stock].Subscribe(t.remote.name, req.etag)
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ch),
			})
			atomic.AddInt32(&t.subscriptionCount, 1)
			err = binary.Write(conn, binary.LittleEndian, common.BLTradeDTO{
				Mix:   common.EncodeCmd(common.CmdSubRes, req.stock),
				AskId: req.handshake,
			})

		default:
			if !ok {
				cases[chosen] = cases[len(cases)-1]
				cases = cases[:len(cases)-1]
				atomic.AddInt32(&t.subscriptionCount, -1)
				continue
			}
			dto := recv.Interface().(*common.BLTradeDTO)
			err = binary.Write(conn, binary.LittleEndian, dto)
		}

		if err != nil {
			Logger.Println("Transport.SendLoop", err)
			conn.Close()
			return
		}
	}
}

func (t *Transport) Allocate(stock int32, etag int32, handshake int32) {
	t.allocates <- TransportAllocateRequest{stock, etag, handshake}
}
