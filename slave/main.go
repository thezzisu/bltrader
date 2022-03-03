package main

import (
	"fmt"
	"runtime"

	lib "github.com/thezzisu/bltrader/slave/lib"
)

func main() {
	lib.Logger.Println("starting...")
	config := lib.LoadSlaveConfig()
	lib.Logger.SetPrefix(fmt.Sprintf("[slave: %s] ", config.Name))
	lib.Logger.Println("config:", config)

	cpus := runtime.NumCPU()
	lib.Logger.Println("cpus:", cpus)
}
