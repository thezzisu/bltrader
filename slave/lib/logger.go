package lib

import (
	"io"
	"log"
	"os"
	"time"
)

var Logger *log.Logger

func init() {
	f, _ := os.Create(time.Now().Format("yyyy-MM-dd_HH-mm-ss") + ".log.txt")
	Logger = log.New(io.MultiWriter(os.Stderr, f), "", log.LstdFlags)
}
