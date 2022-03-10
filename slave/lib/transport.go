package lib

import (
	"encoding/binary"
	"net"
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
		err = binary.Write(conn, binary.BigEndian, Config.Magic)
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
	subscriptions := make([]<-chan *common.BLTradeDTO, 0)
	for {
		select {
		// Just forward commands to slave
		case dto := <-t.remote.command:
			err = binary.Write(conn, binary.LittleEndian, *dto)

		// Allocate a new subscription
		case req := <-t.allocates:
			ch := t.hub.stocks[req.stock].Subscribe(t.remote.name, req.etag)
			subscriptions = append(subscriptions, ch)
			err = binary.Write(conn, binary.LittleEndian, common.BLTradeDTO{
				Mix:   common.EncodeCmd(common.CmdSubRes, req.stock),
				AskId: req.handshake,
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
