package lib

import (
	"bufio"
	"encoding/binary"
	"math/rand"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thezzisu/bltrader/common"
)

type TransportCmd struct {
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
	cmds              chan TransportCmd
}

func CreateTransport(remote *Remote, pair common.RPCPair) *Transport {
	t := new(Transport)
	t.hub = remote.hub
	t.remote = remote
	t.pair = pair

	t.die = make(chan struct{})
	t.incomingConn = make(chan net.Conn)
	t.subscriptionCount = 0
	t.cmds = make(chan TransportCmd)

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
		Logger.Println("Transport\tDialLoop", err)
		t.Close()
		return
	}
	laddr, err := net.ResolveTCPAddr("tcp", t.pair.SlaveAddr)
	if err != nil {
		Logger.Println("Transport\tDialLoop", err)
		t.Close()
		return
	}

	for !t.IsClosed() {
		// TODO add timeout
		conn, err := net.DialTCP("tcp", laddr, raddr)
		if err != nil {
			Logger.Println("Transport\tDialLoop", err)
			time.Sleep(time.Second / 2)
			continue
		}
		err = binary.Write(conn, binary.LittleEndian, Config.Magic)
		if err != nil {
			Logger.Println("Transport\tDialLoop", err)
			conn.Close()
			continue
		}

		// err = conn.SetKeepAlive(true)
		// if err != nil {
		// 	Logger.Println("Transport.DialLoop", err)
		// 	conn.Close()
		// 	continue
		// }
		// err = conn.SetKeepAlivePeriod(time.Second * 10)
		// if err != nil {
		// 	Logger.Println("Transport.DialLoop", err)
		// 	conn.Close()
		// 	continue
		// }

		Logger.Printf("Transport\tDialed connection to %s", conn.RemoteAddr().String())
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
			Logger.Println("Transport\tRecvLoop", err)
			conn.Close()
			return
		}
		t.remote.incoming <- &dto
	}
}

func (t *Transport) SendLoop(conn net.Conn) {
	// TODO consider MTU
	writer := bufio.NewWriterSize(conn, 1400)
	var err error

	const SPECIAL = 3
	cases := make([]reflect.SelectCase, SPECIAL)
	hsids := make([]int32, SPECIAL)
	cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(t.remote.command)}
	cases[1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(t.cmds)}

	remove := func(pos int) {
		cases[pos] = cases[len(cases)-1]
		cases = cases[:len(cases)-1]
		hsids[pos] = hsids[len(hsids)-1]
		hsids = hsids[:len(hsids)-1]
		atomic.AddInt32(&t.subscriptionCount, -1)
	}

	for {
		// flush data every 100ms
		timer := time.NewTimer(time.Millisecond * 100)
		cases[2] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(timer.C),
		}

		chosen, recv, ok := reflect.Select(cases)
		switch chosen {
		case 0: // Handle remote's command
			dto := recv.Interface().(*common.BLTradeDTO)
			err = binary.Write(writer, binary.LittleEndian, dto)

		case 1: // Handle transport's command
			req := recv.Interface().(TransportCmd)
			switch req.stock {
			case -1: // Unsubscribe
				pos := 0
				for i := SPECIAL; i < len(cases); i++ {
					if hsids[i] == req.handshake {
						pos = i
					}
				}
				if pos != 0 {
					remove(pos)
				}

			case -2: // Shape
				if len(cases) <= SPECIAL {
					continue
				}
				remove(rand.Intn(len(cases)-SPECIAL) + SPECIAL)

			default: //Subscribe
				ch := t.hub.stocks[req.stock].Subscribe(t.remote.name, req.etag)
				if ch != nil {
					cases = append(cases, reflect.SelectCase{
						Dir:  reflect.SelectRecv,
						Chan: reflect.ValueOf(ch),
					})
					hsids = append(hsids, req.handshake)
					atomic.AddInt32(&t.subscriptionCount, 1)

					err = binary.Write(writer, binary.LittleEndian, common.BLTradeDTO{
						Mix:   common.EncodeCmd(common.CmdSubRes, req.stock),
						AskId: req.handshake,
					})
				}
			}

		case 2: //Handle timeout
			err = writer.Flush()

		default:
			if !ok {
				remove(chosen)
				if len(cases) <= SPECIAL {
					Logger.Println("Transport\tSendLoop", "request reshape")
					t.remote.reshape <- struct{}{}
				}
				continue
			}
			dto := recv.Interface().(*common.BLTradeDTO)
			err = binary.Write(writer, binary.LittleEndian, dto)
		}

		if !timer.Stop() {
			<-timer.C
		}

		if err != nil {
			Logger.Println("Transport\tSendLoop", err)
			conn.Close()
			return
		}
	}
}

func (t *Transport) Allocate(stock int32, etag int32, handshake int32) {
	atomic.AddInt32(&t.subscriptionCount, 1)
	t.cmds <- TransportCmd{stock, etag, handshake}
}

func (t *Transport) Unallocate(handshake int32) {
	t.cmds <- TransportCmd{-1, 0, handshake}
}

func (t *Transport) Shape() {
	t.cmds <- TransportCmd{-2, 0, 0}
}
