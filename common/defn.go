package common

import "fmt"

type BLOrder struct {
	StkCode   int
	OrderId   int
	Direction int
	Type      int
	Price     float64
	Volume    int
}

const (
	DirBuy  = 1
	DirSell = -1
)

func (order BLOrder) String() string {
	return fmt.Sprintf("Order {\n\tstk = %d\n\torder = %d\n\tdirection = %d\n\ttype = %d\n\tprice = %f\n\tvolume = %d\n}", order.StkCode, order.OrderId, order.Direction, order.Type, order.Price, order.Volume)
}

type BLTrade struct {
	StkCode int
	BidId   int
	AskId   int
	Price   float64
	Volume  int
}

func (trade BLTrade) String() string {
	return fmt.Sprintf("Trade {\n\tstk = %d\n\tbid = %d\n\task = %d\n\tprice = %f\n\tvolume = %d\n}", trade.StkCode, trade.BidId, trade.AskId, trade.Price, trade.Volume)
}
