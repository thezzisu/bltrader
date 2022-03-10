package lib

import (
	"encoding/binary"
	"errors"
	"net"
	"os"
	"sync"
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
	subscriptionCount uint32
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
}

func (t *Transport) AcceptLoop() {
	addr, err := net.ResolveTCPAddr("tcp", t.pair.MasterAddr)
	if err != nil {
		Logger.Println("Transport.AcceptLoop", err)
		t.Close()
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
				break
			}
			conn.SetKeepAlivePeriod(time.Second * 10)
			if err != nil {
				Logger.Println("Transport.AcceptLoop", err)
				break
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
	subscriptions := make([]<-chan *common.BLOrderDTO, 0)
	for {
		select {
		// Just forward commands to slave
		case dto := <-t.remote.command:
			err = binary.Write(conn, binary.LittleEndian, *dto)

		// Allocate a new subscription
		case req := <-t.allocates:
			ch := t.hub.stocks[req.stock].Subscribe(req.etag)
			subscriptions = append(subscriptions, ch)
			err = binary.Write(conn, binary.LittleEndian, common.BLOrderDTO{
				Mix:     common.EncodeCmd(common.CmdSubRes, req.stock),
				OrderId: req.handshake,
			})

		// None-blocking
		default:
		}

		if err != nil {
			Logger.Println("Transport.SendLoop", err)
			conn.Close()
			return
		}

		for i := 0; i < len(subscriptions); i++ {
			select {
			case dto, ok := <-subscriptions[i]:
				if !ok {
					subscriptions[i] = subscriptions[len(subscriptions)-1]
					subscriptions = subscriptions[:len(subscriptions)-1]
					i--
					continue
				}

				err = binary.Write(conn, binary.LittleEndian, *dto)
				if err != nil {
					Logger.Println("Transport.SendLoop", err)
					conn.Close()
					return
				}

			// None-blocking
			default:
			}
		}
	}
}

func (t *Transport) Allocate(stock int32, etag int32, handshake int32) {
	t.allocates <- TransportAllocateRequest{stock, etag, handshake}
}
