package main

import (
	"github.com/thezzisu/bltrader/master/lib"
)

func main() {
	lib.Logger.Println("starting...")
	lib.CheckCache()

	hub := lib.CreateHub()
	hub.MainLoop()
}
