package lib

import (
	"sync"

	"github.com/thezzisu/bltrader/common"
)

// Transport.RecvLoop -> Remote.RecvLoop
var OrderDtoCache *sync.Pool = &sync.Pool{
	New: func() interface{} {
		return new(common.BLOrderDTO)
	},
}

// Remote.RecvLoop -> StockHandler.RecvLoop -> StockHandler.MergeLoop
var OrderCompCache *sync.Pool = &sync.Pool{
	New: func() interface{} {
		return new(common.BLOrderComp)
	},
}

// Remote.command -> Transport.SendLoop
var TradeDtoCache *sync.Pool = &sync.Pool{
	New: func() interface{} {
		return new(common.BLTradeDTO)
	},
}

// StockHandler.MergeLoop -> TradeStore
var TradeCompCache *sync.Pool = &sync.Pool{
	New: func() interface{} {
		return new(common.BLTradeComp)
	},
}
