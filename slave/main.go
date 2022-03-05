package main

import (
	"github.com/thezzisu/bltrader/slave/lib"
)

func main() {
	lib.Logger.Println("starting...")

	hub := lib.CreateHub()
	hub.MainLoop()
}
