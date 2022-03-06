package main

import (
	"fmt"
	"os"

	"github.com/thezzisu/bltrader/common"
	"github.com/thezzisu/bltrader/master/lib"
)

func main() {
	count := lib.ChunkCount
	os.MkdirAll(".data", 0700)
	for stock := 0; stock < 10; stock++ {
		fmt.Printf("Stock %d\n", stock)
		hooks := lib.LoadHooks(int32(stock))
		f, _ := os.Create(fmt.Sprintf(".data/hook-%d.txt", stock))
		f.WriteString(fmt.Sprintf("%d\n", len(hooks)))
		for _, hook := range hooks {
			f.WriteString(fmt.Sprintf("%d %d %d %d\n", hook.SelfOrderId, hook.TargetStkCode, hook.TargetTradeIdx, hook.Arg))
		}
		f.Close()
		chunks := make([]common.BLOrder, 0)
		for i := 0; i < count; i++ {
			chunks = append(chunks, lib.LoadOrderChunk(int32(stock), i)...)
		}
		f, _ = os.Create(fmt.Sprintf(".data/input-%d.txt", stock))
		f.WriteString(fmt.Sprintf("%d\n", len(chunks)))
		for _, order := range chunks {
			f.WriteString(fmt.Sprintf("%d %d %d %d %f %d\n", order.StkCode, order.OrderId, order.Direction, order.Type, order.Price, order.Volume))
		}
		f.Close()
	}
}
