package main

import (
	"runtime"

	"github.com/thezzisu/bltrader/common"
	"github.com/thezzisu/bltrader/master/lib"
)

func main() {
	runtime.GOMAXPROCS(lib.Config.Procs)
	lib.Logger.Printf("using %d cores\n", lib.Config.Procs)
	lib.CheckCache()

	chunk := lib.LoadOrderChunk(0, 0)
	for _, order := range chunk {
		var dto common.BLOrderDTO
		common.MarshalOrderDTO(&order, &dto)
		var res common.BLOrder
		common.UnmarshalOrderDTO(&dto, &res)
		if order.StkCode != res.StkCode {
			lib.Logger.Fatalf("%d != %d", order.StkCode, res.StkCode)
		}
		if order.OrderId != res.OrderId {
			lib.Logger.Fatalf("%d != %d", order.OrderId, res.OrderId)
		}
		if order.Direction != res.Direction {
			lib.Logger.Fatalf("%d != %d", order.Direction, res.Direction)
		}
		if order.Type != res.Type {
			lib.Logger.Fatalf("%d != %d", order.Type, res.Type)
		}
		if order.Price != res.Price {
			lib.Logger.Fatalf("%f != %f", order.Price, res.Price)
		}
		if order.Volume != res.Volume {
			lib.Logger.Fatalf("%d != %d", order.Volume, res.Volume)
		}
	}
	lib.Logger.Println("passed")
	// hub := lib.CreateHub()
	// hub.MainLoop()
}
