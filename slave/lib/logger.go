package lib

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

var Logger *log.Logger

func init() {
	now := time.Now()
	f, _ := os.Create(fmt.Sprintf("%d-%d-%d_%d-%d-%d.slave.log.txt", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second()))
	Logger = log.New(io.MultiWriter(os.Stderr, f), "", log.LstdFlags)
}
