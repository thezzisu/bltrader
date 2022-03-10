package lib

import (
	"encoding/binary"
	"errors"
	"net"
	"os"
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
	go t.AcceptLoop()
	go t.HandleLoop()
}

func (t *Transport) AcceptLoop() {
	addr, err := net.ResolveTCPAddr("tcp", t.pair.MasterAddr)
	if err != nil {
		Logger.Println("Transport.AcceptLoop", err)
		t.Close()
		return
	}
	for !t.IsClosed() {
		listener, err := net.ListenTCP("tcp", addr)
		if err != nil {
			Logger.Println("Transport.AcceptLoop", err)
			time.Sleep(time.Second / 2)
			continue
		}
		Logger.Printf("Transport listening on %s", t.pair.MasterAddr)
		for !t.IsClosed() {
			// TODO add configuraion for timeout
			listener.SetDeadline(time.Now().Add(time.Second))
			conn, err := listener.AcceptTCP()
			if errors.Is(err, os.ErrDeadlineExceeded) {
				continue
			}
			if err != nil {
				Logger.Println("Transport.AcceptLoop", err)
				break
			}

			// Verify connection
			var magic uint32
			err = binary.Read(conn, binary.LittleEndian, &magic)
			if err != nil || magic != Config.Magic {
				Logger.Printf("Invalid connection from %s", conn.RemoteAddr())
				conn.Close()
				continue
			}

			// Set socket options
			err = conn.SetKeepAlive(true)
			if err != nil {
				Logger.Println("Transport.AcceptLoop", err)
				conn.Close()
				continue
			}
			// TODO add configuration
			conn.SetKeepAlivePeriod(time.Second * 10)
			if err != nil {
				Logger.Println("Transport.AcceptLoop", err)
				conn.Close()
				continue
			}

			Logger.Printf("Transport accepted connection from %s", conn.RemoteAddr().String())
			t.incomingConn <- conn
		}
		listener.Close()
	}
	close(t.incomingConn)
}

func (t *Transport) HandleLoop() {
	var lastConn net.Conn
	for !t.IsClosed() {
		select {
		case newConn, ok := <-t.incomingConn:
			if !ok {
				return
			}
			if lastConn != nil {
				lastConn.Close()
			}
			go t.RecvLoop(newConn)
			go t.SendLoop(newConn)
			lastConn = newConn
		case <-t.die:
			if lastConn != nil {
				lastConn.Close()
			}
			return
		}
	}
}

func (t *Transport) RecvLoop(conn net.Conn) {
	for {
		var dto common.BLTradeDTO
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
			dto := recv.Interface().(*common.BLOrderDTO)
			err = binary.Write(conn, binary.LittleEndian, dto)

		case 1: // Handle allocate
			req := recv.Interface().(TransportAllocateRequest)
			ch := t.hub.stocks[req.stock].Subscribe(req.etag)
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ch),
			})
			atomic.AddInt32(&t.subscriptionCount, 1)
			err = binary.Write(conn, binary.LittleEndian, common.BLOrderDTO{
				Mix:     common.EncodeCmd(common.CmdSubRes, req.stock),
				OrderId: req.handshake,
			})

		default:
			if !ok {
				cases[chosen] = cases[len(cases)-1]
				cases = cases[:len(cases)-1]
				atomic.AddInt32(&t.subscriptionCount, -1)
				continue
			}
			dto := recv.Interface().(*common.BLOrderDTO)
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
