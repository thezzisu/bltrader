package lib

import (
	"fmt"
	"time"

	"github.com/thezzisu/bltrader/common"
)

type Peeker struct {
	ch   chan *common.BLOrder
	last *common.BLOrder
}

func CreatePeeker() *Peeker {
	ch := make(chan *common.BLOrder)
	return &Peeker{ch: ch}
}

func Peek(p *Peeker) *common.BLOrder {
	if p.last == nil {
		p.last = <-p.ch
	}
	return p.last
}

func Get(p *Peeker) *common.BLOrder {
	if p.last == nil {
		return <-p.ch
	}
	last := p.last
	p.last = nil
	return last
}

type TradeReader struct {
	//
}

func CreateTradeReader() *TradeReader {
	t := new(TradeReader)
	return t
}

func (t *TradeReader) Seek(etag int32) {
	//
}

func (t *TradeReader) Next() *common.BLTrade {
	//
	return nil
}

type StockSubscribeRequest struct {
	etag int32
	ch   chan *common.BLTradeDTO
}

type StockHandler struct {
	hub        *Hub
	stockId    int32
	subscribes map[string]chan *StockSubscribeRequest
	peekers    map[string]*Peeker
	readers    map[string]*TradeReader
}

func CreateStockHandler(hub *Hub, stockId int32) *StockHandler {
	sh := new(StockHandler)
	sh.hub = hub
	sh.stockId = stockId
	sh.subscribes = make(map[string]chan *StockSubscribeRequest)
	sh.peekers = make(map[string]*Peeker)
	return sh
}

func (sh *StockHandler) Subscribe(name string, etag int32) <-chan *common.BLTradeDTO {
	ch := make(chan *common.BLTradeDTO)
	sh.subscribes[name] <- &StockSubscribeRequest{etag: etag, ch: ch}
	return ch
}

func (sh *StockHandler) SendLoop(name string) {
	ch := make(chan *common.BLTradeDTO)
	subscribe := sh.subscribes[name]
	reader := sh.readers[name]

	replace := func(req *StockSubscribeRequest) {
		fmt.Printf("StockHandler.SendLoop(%s): subscribing to %d\n", name, req.etag)
		close(ch)
		ch = req.ch
		reader.Seek(req.etag)
	}

	for {
		trade := reader.Next()

		if trade == nil {
			// Send finished
			// Write EOF to remote
			dto := new(common.BLTradeDTO)
			common.MarshalTradeDTO(&common.BLTrade{
				StkCode: sh.stockId,
				AskId:   -1,
			}, dto)

			select {
			// New subscriber
			case req := <-subscribe:
				replace(req)

			// EOF sent, waiting for new subscriber
			case ch <- dto:
				req := <-subscribe
				replace(req)
			}
		}

		dto := new(common.BLTradeDTO)
		common.MarshalTradeDTO(trade, dto)

		select {
		case req := <-subscribe:
			replace(req)
		case ch <- dto:
		}
	}
}

func (sh *StockHandler) RecvLoop(name string) {
	remote := sh.hub.remotes[name]
	peeker := sh.peekers[name]
	etag := int32(0)
subscribe:
	for {
		ch := remote.Subscribe(sh.stockId, etag)
		for {
			// TODO add configuration for timeout
			timer := time.NewTimer(time.Second * 10)
			select {
			case order, ok := <-ch:
				if !ok {
					break
				}
				if order.OrderId == -1 {
					break subscribe
				}
				etag = order.OrderId
				peeker.ch <- order

			case <-timer.C:
				Logger.Printf("StockHandler[%d].RecvLoop(%s) timeout\n", sh.stockId, name)
				continue subscribe
			}
		}
	}
	sh.hub.wg.Done()
}

func (sh *StockHandler) Start() {
	for _, master := range Config.Masters {
		sh.subscribes[master.Name] = make(chan *StockSubscribeRequest)
		sh.peekers[master.Name] = CreatePeeker()
		sh.readers[master.Name] = CreateTradeReader()

		sh.hub.wg.Add(1)
		go sh.SendLoop(master.Name)
		go sh.RecvLoop(master.Name)
	}
}
