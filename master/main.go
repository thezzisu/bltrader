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
	chunk := lib.LoadOrderChunk(9, 0)
	fmt.Println(len(chunk))
}
