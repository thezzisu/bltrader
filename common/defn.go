package common

import (
	"fmt"
	"math"
)

const (
	CmdSubReq = 0
	CmdSubRes = 1
	CmdUnsub  = 2
)

func IsCmd(mix int32) bool {
	return mix < 0
}

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
	Mix     int32
	OrderId int32
	Price   int32
}

func MarshalOrderDTO(order *BLOrder, dto *BLOrderDTO) {
	mix := order.Volume
	mix |= order.StkCode << 24
	if order.Direction == -1 {
		mix |= 1 << 23
	}
	mix |= order.Type << 20

	dto.Mix = mix
	dto.OrderId = order.OrderId
	dto.Price = int32(math.Round(order.Price * 100))
}

func UnmarshalOrderDTO(dto *BLOrderDTO, order *BLOrder) {
	var direction int32 = 1
	if dto.Mix&(1<<23) != 0 {
		direction = -1
	}
	order.StkCode = dto.Mix >> 24
	order.OrderId = dto.OrderId
	order.Direction = direction
	order.Type = dto.Mix >> 20 & 7
	order.Price = float64(dto.Price) / 100
	order.Volume = dto.Mix & 0xfffff
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
	Mix   int32
	BidId int32
	AskId int32
	Price int32
}

func MarshalTradeDTO(trade *BLTrade, dto *BLTradeDTO) {
	mix := trade.Volume
	mix |= trade.StkCode << 24

	dto.Mix = mix
	dto.BidId = trade.BidId
	dto.AskId = trade.AskId
	dto.Price = int32(math.Round(trade.Price * 100))
}

func UnmarshalTradeDTO(dto *BLTradeDTO, trade *BLTrade) {
	trade.StkCode = dto.Mix >> 24
	trade.BidId = dto.BidId
	trade.AskId = dto.AskId
	trade.Price = float64(dto.Price) / 100
	trade.Volume = dto.Mix & 0xfffff
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
