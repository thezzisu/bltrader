package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"runtime"

	"github.com/thezzisu/bltrader/master/lib"
)

func main() {
	go func() {
		fmt.Println(http.ListenAndServe("localhost:61926", nil))
	}()
	flag.Parse()

	runtime.GOMAXPROCS(lib.Config.Procs)
	fmt.Printf("using %d cores\n", lib.Config.Procs)
	lib.InitConfig()
	lib.InitDataset()
	lib.CheckCache()

	hub := lib.CreateHub()
	hub.Start()
}
