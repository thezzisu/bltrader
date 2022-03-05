package main

import (
	"fmt"

	"github.com/thezzisu/bltrader/master/lib"
)

func main() {
	lib.Logger.Println("starting...")
	lib.CheckCache()
	chunk := lib.LoadOrderChunk(9, 0)
	fmt.Println(len(chunk), cap(chunk))
	fmt.Println(chunk[0])
	hooks := lib.LoadHooks(0)
	fmt.Println(hooks[0])
}
