package lib

import (
	"bufio"
	"encoding/binary"
	"errors"
	"math/rand"
	"net"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thezzisu/bltrader/common"
)

type TransportCmd struct {
	stock int32
	etag  int32
	sid   int16
}

type TransportSubscription struct {
	stock int32
	sid   int16
}

type Transport struct {
	hub               *Hub
	remote            *Remote
	id                int
	pair              common.RPCPair
	die               chan struct{}
	dieOnce           sync.Once
	incomingConn      chan net.Conn
	subscriptionCount int32
	ready             int32
	cmds              chan TransportCmd
}

func CreateTransport(remote *Remote, id int, pair common.RPCPair) *Transport {
	t := new(Transport)
	t.hub = remote.hub
	t.remote = remote
	t.id = id
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
	go t.AcceptLoop()
	go t.HandleLoop()
}

func (t *Transport) AcceptLoop() {
	addr, err := net.ResolveTCPAddr("tcp", t.pair.MasterAddr)
	if err != nil {
		Logger.Println("Transport\tAcceptLoop", err)
		t.Close()
		return
	}
	for !t.IsClosed() {
		listener, err := net.ListenTCP("tcp", addr)
		if err != nil {
			Logger.Println("Transport\tAcceptLoop", err)
			time.Sleep(time.Second / 2)
			continue
		}
		Logger.Printf("Transport\tListening on %s", t.pair.MasterAddr)
		for !t.IsClosed() {
			// TODO add configuraion for timeout
			listener.SetDeadline(time.Now().Add(time.Second))
			conn, err := listener.AcceptTCP()
			if errors.Is(err, os.ErrDeadlineExceeded) {
				continue
			}
			if err != nil {
				Logger.Println("Transport\tAcceptLoop", err)
				break
			}

			// Verify connection
			var magic uint32
			err = binary.Read(conn, binary.LittleEndian, &magic)
			if err != nil || magic != Config.Magic {
				Logger.Printf("Transport\tInvalid connection from %s", conn.RemoteAddr())
				conn.Close()
				continue
			}

			// Set socket options
			// err = conn.SetKeepAlive(true)
			// if err != nil {
			// 	Logger.Println("Transport.AcceptLoop", err)
			// 	conn.Close()
			// 	continue
			// }
			// conn.SetKeepAlivePeriod(time.Second * 10)
			// if err != nil {
			// 	Logger.Println("Transport.AcceptLoop", err)
			// 	conn.Close()
			// 	continue
			// }

			Logger.Printf("Transport\taccepted connection from %s", conn.RemoteAddr().String())
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
			atomic.StoreInt32(&t.ready, 1)
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
			Logger.Println("Transport\tRecvLoop", err)
			conn.Close()
			return
		}
		t.remote.incoming <- RemotePacket{
			src:  t.id,
			data: &dto,
		}
	}
}

func (t *Transport) SendLoop(conn net.Conn) {
	writer := bufio.NewWriterSize(conn, Config.SendBufferSize)
	timeout := time.Duration(Config.FlushIntervalMs) * time.Millisecond
	var err error

	const SPECIAL = 3
	cases := make([]reflect.SelectCase, SPECIAL)
	subs := make([]TransportSubscription, SPECIAL)
	cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(t.remote.command)}
	cases[1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(t.cmds)}

	remove := func(pos int) {
		cases[pos] = cases[len(cases)-1]
		cases = cases[:len(cases)-1]
		subs[pos] = subs[len(subs)-1]
		subs = subs[:len(subs)-1]
		atomic.AddInt32(&t.subscriptionCount, -1)
	}

	for {
		timer := time.NewTimer(timeout)
		cases[2] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(timer.C),
		}

		chosen, recv, ok := reflect.Select(cases)
		switch chosen {
		case 0: // Handle remote's command
			if !timer.Stop() {
				<-timer.C
			}
			dto := recv.Interface().(*common.BLOrderDTO)
			err = binary.Write(writer, binary.LittleEndian, dto)

		case 1: // Handle transport's command
			if !timer.Stop() {
				<-timer.C
			}
			req := recv.Interface().(TransportCmd)
			switch req.stock {
			case -1: // Unsubscribe
				pos := 0
				for i := SPECIAL; i < len(cases); i++ {
					if subs[i].sid == req.sid {
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
				pos := 0
				for i := SPECIAL; i < len(cases); i++ {
					if subs[i].stock == req.stock {
						pos = i
					}
				}
				if pos != 0 {
					remove(pos)
				}
				ch := t.hub.stocks[req.stock].Subscribe(req.etag)
				if ch != nil {
					cases = append(cases, reflect.SelectCase{
						Dir:  reflect.SelectRecv,
						Chan: reflect.ValueOf(ch),
					})
					subs = append(subs, TransportSubscription{
						stock: req.stock,
						sid:   req.sid,
					})
					atomic.AddInt32(&t.subscriptionCount, 1)

					err = binary.Write(writer, binary.LittleEndian, common.BLOrderDTO{
						Sid:    -common.CmdSubRes,
						Volume: req.sid,
					})
				}
			}

		case 2: //Handle timeout
			err = writer.Flush()

		default:
			if !timer.Stop() {
				<-timer.C
			}
			if !ok {
				remove(chosen)
				if len(cases) <= SPECIAL {
					Logger.Println("Transport\tSendLoop", "request reshape")
					t.remote.reshape <- struct{}{}
				}
				continue
			}
			order := recv.Interface().(*common.BLOrder)
			var dto common.BLOrderDTO
			common.MarshalOrderDTO(subs[chosen].sid, order, &dto)
			err = binary.Write(writer, binary.LittleEndian, dto)
		}

		if err != nil {
			Logger.Println("Transport\tSendLoop", err)
			conn.Close()
			return
		}
	}
}

func (t *Transport) Allocate(stock int32, etag int32, sid int16) {
	t.cmds <- TransportCmd{stock, etag, sid}
}

func (t *Transport) Unallocate(sid int16) {
	t.cmds <- TransportCmd{-1, 0, sid}
}

func (t *Transport) Shape() {
	t.cmds <- TransportCmd{-2, 0, 0}
}
