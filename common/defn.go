package common

import (
	"fmt"
	"math"
)

func PriceF2I(price float64) int32 {
	return int32(math.Round(price * 100))
}

func PriceI2F(price int32) float64 {
	return float64(price) / 100
}

const (
	CmdSubReq = 1
	CmdSubRes = 2
	CmdUnsub  = 3
)

func EncodeCmd(cmd int32, payload int32) int32 {
	return -((cmd<<16 | payload) + 1)
}

func DecodeCmd(mix int32) (int32, int32) {
	mix = -mix - 1
	return mix >> 16, mix & 0xffff
}

type BLOrder struct {
	StkCode   int32
	OrderId   int32
	Direction int32 //1 for buy, -1 for sell
	Type      int32
	Price     float64
	Volume    int32
}

const (
	DirBuy  = 1
	DirSell = -1
)

func (order BLOrder) String() string {
	return fmt.Sprintf("Order {\n\tstk = %d\n\torder = %d\n\tdirection = %d\n\ttype = %d\n\tprice = %f\n\tvolume = %d\n}", order.StkCode, order.OrderId, order.Direction, order.Type, order.Price, order.Volume)
}

type BLOrderDTO struct {
	Sid     int16
	Mix     int8
	Volume  int16
	OrderId int32
	Price   int32
}

func MarshalOrderDTO(sid int16, order *BLOrder, dto *BLOrderDTO) {
	dto.Sid = sid
	var mix int8 = int8(order.Type)
	if order.Direction == -1 {
		mix |= 1 << 3
	}
	dto.Mix = mix
	dto.Volume = int16(order.Volume)
	dto.OrderId = order.OrderId
	dto.Price = PriceF2I(order.Price)
}

func UnmarshalOrderDTO(stock int32, dto *BLOrderDTO, order *BLOrder) {
	order.StkCode = stock
	order.OrderId = dto.OrderId
	order.Direction = 1
	if dto.Mix&(1<<3) != 0 {
		order.Direction = -1
	}
	order.Type = int32(dto.Mix & 7)
	order.Price = PriceI2F(dto.Price)
	order.Volume = int32(dto.Volume)
}

type BLTrade struct {
	StkCode int32
	BidId   int32 //买方
	AskId   int32 //卖方
	Price   float64
	Volume  int32
}

func (trade BLTrade) String() string {
	return fmt.Sprintf("Trade {\n\tstk = %d\n\tbid = %d\n\task = %d\n\tprice = %f\n\tvolume = %d\n}", trade.StkCode, trade.BidId, trade.AskId, trade.Price, trade.Volume)
}

type BLTradeDTO struct {
	Sid    int16
	Volume int16
	BidId  int32
	AskId  int32
	Price  int32
}

func MarshalTradeDTO(sid int16, trade *BLTrade, dto *BLTradeDTO) {
	dto.Sid = sid
	dto.Volume = int16(trade.Volume)
	dto.BidId = trade.BidId
	dto.AskId = trade.AskId
	dto.Price = PriceF2I(trade.Price)
}

func UnmarshalTradeDTO(stock int32, dto *BLTradeDTO, trade *BLTrade) {
	trade.StkCode = stock
	trade.BidId = dto.BidId
	trade.AskId = dto.AskId
	trade.Price = PriceI2F(dto.Price)
	trade.Volume = int32(dto.Volume)
}

type BLHook struct {
	SelfOrderId    int32
	TargetStkCode  int32
	TargetTradeIdx int32
	Arg            int32
}

func (hook BLHook) String() string {
	return fmt.Sprintf("Hook {\n\tself_order = %d\n\ttarget_stk = %d\n\ttarget_trade = %d\n\targ = %d\n}", hook.SelfOrderId, hook.TargetStkCode, hook.TargetTradeIdx, hook.Arg)
}

type BLTradeComp struct {
	BidId  int32
	AskId  int32
	Price  int32
	Volume int16
}

func (trade BLTradeComp) String() string {
	return fmt.Sprintf("Trade {\n\tbid = %d\n\task = %d\n\tprice = %f\n\tvolume = %d\n}", trade.BidId, trade.AskId, PriceI2F(trade.Price), trade.Volume)
}
