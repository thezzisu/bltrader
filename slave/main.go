package main

import (
	"fmt"
	"runtime"

	"github.com/thezzisu/bltrader/slave/lib"
)

func main() {
	lib.Logger.Println("starting...")
	config := lib.LoadSlaveConfig()
	lib.Logger.SetPrefix(fmt.Sprintf("[slave: %s] ", config.Name))
	lib.Logger.Println("configuration loaded")

	cpus := runtime.NumCPU()
	lib.Logger.Println("cpus:", cpus)
}
