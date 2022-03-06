package main

import (
	"runtime"

	"github.com/thezzisu/bltrader/slave/lib"
)

func main() {
	runtime.GOMAXPROCS(lib.Config.Procs)
	lib.Logger.Printf("using %d cores\n", lib.Config.Procs)

	hub := lib.CreateHub()
	hub.MainLoop()
}
