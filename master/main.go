package main

import (
	"fmt"

	"github.com/thezzisu/bltrader/master/lib"
)

func main() {
	lib.Logger.Println("starting...")
	lib.Logger.SetPrefix(fmt.Sprintf("[master %s] ", lib.Config.Name))
	lib.Logger.Println("configuration loaded")
	lib.CheckCache()
	prices := lib.LoadDatasetFloat64(9, 0, "price")
	fmt.Println(prices)
}
