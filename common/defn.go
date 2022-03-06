package common

import "fmt"

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
	OrderId   int32
	Direction int32
	Type      int32
	Price     float64
	Volume    int32
}

type BLTrade struct {
	StkCode int32
	TradeId int32 // **starting from 1** for each stock
	BidId   int32 //买方
	AskId   int32 //卖方
	Price   float64
	Volume  int32
}

func (trade BLTrade) String() string {
	return fmt.Sprintf("Trade {\n\tstk = %d\n\ttxn = %d\n\tbid = %d\n\task = %d\n\tprice = %f\n\tvolume = %d\n}", trade.StkCode, trade.TradeId, trade.BidId, trade.AskId, trade.Price, trade.Volume)
}

type BLTradeDTO struct {
	BidId  int32
	AskId  int32
	Price  float64
	Volume int32
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
