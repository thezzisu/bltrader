package lib

import (
	"bufio"
	"encoding/binary"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thezzisu/bltrader/common"
)

type TransportCmd struct {
	stock int32
	sid   int16
	ch    <-chan *common.BLTradeComp
}

type TransportSubscription struct {
	stock int32
	sid   int16
	ts    uint16
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
	pendingCount      int32
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
	atomic.StoreInt32(&t.ready, 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { t.RecvLoop(conn); wg.Done() }()
	go func() { t.SendLoop(conn); wg.Done() }()
	wg.Wait()
	atomic.StoreInt32(&t.ready, 0)
}

func (t *Transport) RecvLoop(conn net.Conn) {
	for {
		dto := OrderDtoCache.Get().(*common.BLOrderDTO)
		err := binary.Read(conn, binary.LittleEndian, dto)
		if err != nil {
			Logger.Println("Transport\tRecvLoop", err)
			conn.Close()
			return
		}
		t.remote.incoming <- RemotePacket{
			src:  t.id,
			data: dto,
		}
	}
}

func (t *Transport) SendLoop(conn net.Conn) {
	var stamp uint16 = 0
	nextStamp := func() uint16 {
		stamp++
		return stamp
	}
	writer := bufio.NewWriterSize(conn, Config.SendBufferSize)
	timeout := time.Duration(Config.FlushIntervalMs) * time.Millisecond
	ticker := time.NewTicker(timeout)
	var err error

	const SPECIAL = 3
	cases := make([]reflect.SelectCase, SPECIAL)
	subs := make([]TransportSubscription, SPECIAL)
	cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(t.remote.command)}
	cases[1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(t.cmds)}
	cases[2] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ticker.C)}

	remove := func(pos int) {
		cases[pos] = cases[len(cases)-1]
		cases = cases[:len(cases)-1]
		subs[pos] = subs[len(subs)-1]
		subs = subs[:len(subs)-1]
		atomic.AddInt32(&t.subscriptionCount, -1)
	}

	for {
		chosen, recv, ok := reflect.Select(cases)
		switch chosen {
		case 0: // Handle remote's command
			dto := recv.Interface().(*common.BLTradeDTO)
			err = binary.Write(writer, binary.LittleEndian, dto)
			TradeDtoCache.Put(dto)

		case 1: // Handle transport's command
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
				pos := SPECIAL
				for i := SPECIAL + 1; i < len(cases); i++ {
					// Choose the oldest subscription
					if subs[i].ts < subs[pos].ts {
						pos = i
					}
				}
				remove(pos)

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
				cases = append(cases, reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(req.ch),
				})
				subs = append(subs, TransportSubscription{
					stock: req.stock,
					sid:   req.sid,
					ts:    nextStamp(),
				})
				atomic.AddInt32(&t.subscriptionCount, 1)
				atomic.AddInt32(&t.pendingCount, -1)

				err = binary.Write(writer, binary.LittleEndian, common.BLTradeDTO{
					Sid:    -common.CmdSubRes,
					Volume: req.sid,
				})
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
			trade := recv.Interface().(*common.BLTradeComp)
			dto := TradeDtoCache.Get().(*common.BLTradeDTO)
			dto.Sid = subs[chosen].sid
			dto.Volume = trade.Volume
			dto.BidId = trade.BidId
			dto.AskId = trade.AskId
			dto.Price = trade.Price
			err = binary.Write(writer, binary.LittleEndian, dto)
			TradeDtoCache.Put(dto)
		}

		if err != nil {
			Logger.Println("Transport\tSendLoop", err)
			conn.Close()
			return
		}
	}
}

func (t *Transport) Add(stock int32, sid int16, ch <-chan *common.BLTradeComp) {
	atomic.AddInt32(&t.pendingCount, 1)
	t.cmds <- TransportCmd{stock, sid, ch}
}

func (t *Transport) Unallocate(sid int16) {
	t.cmds <- TransportCmd{-1, 0, nil}
}

func (t *Transport) Shape() {
	t.cmds <- TransportCmd{-2, 0, nil}
}
