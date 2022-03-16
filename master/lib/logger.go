package lib

import (
	"io"
	"log"
	"os"
)

var Logger *log.Logger

func init() {
	f, _ := os.Create("log.txt")
	Logger = log.New(io.MultiWriter(os.Stderr, f), "", log.LstdFlags)
}
