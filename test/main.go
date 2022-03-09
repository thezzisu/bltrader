package main

import (
	"fmt"
	"time"

	"github.com/thezzisu/bltrader/common"
	"github.com/thezzisu/bltrader/core"
)

// #include "bf.h"
import "C"

func main() {
	ordn := 9990
	tran := 5541
	hn := 56
	orders := make([]common.BLOrder, ordn)
	for i := 0; i < ordn; i++ {
		orders[i] = common.BLOrder{StkCode: 0, OrderId: int32(C.int(C.so[i])), Direction: int32(C.int(C.sd[i])), Type: int32(C.int(C.st[i])), Price: float64(C.double(C.sp[i])), Volume: int32(C.int(C.sv[i]))}
	}
	atrade := make([]common.BLTrade, tran)
	for i := 0; i < tran; i++ {
		atrade[i] = common.BLTrade{StkCode: 0, BidId: int32(C.int(C.tb[i])), AskId: int32(C.int(C.ta[i])), Price: float64(C.double(C.tp[i])), Volume: int32(C.int(C.tv[i]))}
	}
	forb := []int32{5036, 5235, 5248, 5283, 5314, 5659, 5755, 5829, 5852, 6096, 6151, 6230, 6611, 6677, 6706, 6714, 6858, 6881, 6883, 6951, 6966, 7170, 7330, 7403, 7410, 7423, 7471, 7647, 7670, 7697, 7849, 7912, 7952, 7992, 8346, 8386, 8580, 8609, 8632, 8659, 8668, 8764, 8928, 8940, 8951, 8963, 8995, 9055, 9072, 9151, 9191, 9362, 9410, 9438, 9816, 9847}
	fob := make(map[int32]bool)
	for i := 0; i < hn; i++ {
		fob[forb[i]] = true
	}
	btrade := make([]common.BLTrade, 0)
	blr := new(core.BLRunner)
	t1 := time.Now()
	bp := 5000
	isdp := false
	func(breakpoint int, isDump bool) {
		blr.Load(-10000.0, 10000.0)
		if isDump {
			for i := 0; i < ordn; i++ {
				if i == breakpoint && i > 0 {
					blr.Dump()
					fmt.Printf("Succeeded in Dump,the trade size = %d\n", len(btrade))
				}
				if _, ok := fob[orders[i].OrderId]; ok {
					continue
				}
				btrade = append(btrade, blr.Dispatch(&orders[i])...)
			}
		} else {
			for i := breakpoint; i < ordn; i++ {
				if _, ok := fob[orders[i].OrderId]; ok {
					continue
				}
				btrade = append(btrade, blr.Dispatch(&orders[i])...)
			}
		}
	}(bp, isdp)
	t2 := time.Now()
	toffset := 2991
	if isdp {
		toffset = 0
	}
	fmt.Printf("main part's time %d (ms)\n", t2.Sub(t1).Milliseconds())
	fmt.Printf("AnsTrades %d,MyTrades %d\n", tran, len(btrade))
	tnn := tran
	if tnn > len(btrade)+toffset {
		tnn = len(btrade) + toffset
	}
	i := 0
	for i = toffset; i < tnn; i++ {
		if atrade[i].BidId != btrade[i-toffset].BidId {
			fmt.Printf("Differ At: %d\n", i)
			fmt.Println(atrade[i].String())
			fmt.Println(btrade[i-toffset].String())
			break
		}
		if atrade[i].AskId != btrade[i-toffset].AskId {
			fmt.Printf("Differ At: %d\n", i)
			fmt.Println(atrade[i].String())
			fmt.Println(btrade[i-toffset].String())
			break
		}
		if atrade[i].Price != btrade[i-toffset].Price {
			fmt.Printf("Differ At: %d\n", i)
			fmt.Println(atrade[i].String())
			fmt.Println(btrade[i-toffset].String())
			break
		}
		if atrade[i].Volume != btrade[i-toffset].Volume {
			fmt.Printf("Differ At: %d\n", i)
			fmt.Println(atrade[i].String())
			fmt.Println(btrade[i-toffset].String())
			break
		}
	}
	if i == tnn {
		fmt.Println("All Correct!")
	}
}
