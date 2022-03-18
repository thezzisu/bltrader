package main

import (
	"flag"
	"fmt"
	"runtime"

	"github.com/thezzisu/bltrader/master/lib"
)

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(lib.Config.Procs)
	fmt.Printf("using %d cores\n", lib.Config.Procs)
	lib.InitConfig()
	lib.InitDataset()
	lib.CheckCache()

	hub := lib.CreateHub()
	hub.Start()
}
