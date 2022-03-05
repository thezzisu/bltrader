package lib

import (
	"errors"
	"log"
)

var Logger = log.New(log.Writer(), "", log.LstdFlags)

var (
	ErrAgain = errors.New("try again")
)
