package core

import "github.com/thezzisu/bltrader/common"

type IBLRunner interface {
	Dispatch(order common.BLOrder) []common.BLTrade

	Load()
	Dump()
}
