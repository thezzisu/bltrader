package main

import (
	"fmt"

	"github.com/thezzisu/bltrader/master/lib"
)

func main() {
	lib.Logger.Println("starting...")
	config := lib.LoadMasterConfig()
	lib.Logger.SetPrefix(fmt.Sprintf("[master %s] ", config.Name))
	lib.Logger.Println("configuration loaded")
}
