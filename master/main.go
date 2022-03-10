package main

import (
	"runtime"

	"github.com/thezzisu/bltrader/master/lib"
)

func main() {
	runtime.GOMAXPROCS(lib.Config.Procs)
	lib.Logger.Printf("using %d cores\n", lib.Config.Procs)
	lib.CheckCache()

	hub := lib.CreateHub()
	hub.Start()
}
