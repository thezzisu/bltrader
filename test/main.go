package main

import (
	"fmt"

	"github.com/thezzisu/bltrader/common"
)

func main() {
	order := common.BLOrder{StkCode: 1, OrderId: 1, Direction: 1, Type: 1, Price: 1, Volume: 1}
	fmt.Println(order)
}
