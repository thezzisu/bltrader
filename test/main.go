package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/thezzisu/bltrader/common"
	"github.com/thezzisu/bltrader/core"
)

/*
trade_i
	trade_num int32
	bid int32	ask int32	price float64	volume int32
*/

/*
order_1 & order_2
	order_num int32
	orderid int32	direction int32	type int32	price float64	volume int32
*/

/*
hook
	order_num int32	stkcode	int32	trade_id int32	volarg int32
*/

func ReadLine(br *bufio.Reader) (string, bool) {
	sc, err := br.ReadString('\n')
	if err != nil {
		return "", true
	}
	return sc, false
}

func main() {
	datadir := "./data/"
	//var orders [][]common.BLOrder
	//orders = make([][]common.BLOrder, 2)
	aorder := make([]common.BLOrder, 0)
	//order_size := make([]int, 2)
	var aosize int
	var trades [][]common.BLTrade
	trades = make([][]common.BLTrade, 10)
	trade_size := make([]int, 10)
	/*
		hooks := make([]common.BLHook, 0)
		var hook_size int32
	*/
	timep1 := time.Now()
	var br *bufio.Reader
	func() {
		orderFile, err := os.OpenFile(datadir+"stock-3.txt", os.O_RDONLY, 0777)
		if err != nil {
			panic("Failed to read order file")
		}
		br = bufio.NewReader(orderFile)
		line, end := ReadLine(br)
		if end {
			panic("Order file corrupted")
		}
		cnt := 0
		for {
			line, end = ReadLine(br)
			if end {
				break
			}
			var od common.BLOrder
			var o int32
			_, _ = fmt.Sscanf(line, "%d%d%d%d%f%d", &o, &od.OrderId, &od.Direction, &od.Type, &od.Price, &od.Volume)
			aorder = append(aorder, od)
			cnt++
		}
		aosize = len(aorder)
	}()
	func() {
		for i := 3; i == 3; i++ {
			trades[i] = make([]common.BLTrade, 0)
			tradeFile, err := os.OpenFile(datadir+"trade03-", os.O_RDONLY, 0777)
			if err != nil {
				panic("Failed to read trade file")
			}
			br = bufio.NewReader(tradeFile)
			line, end := ReadLine(br)
			if end {
				panic("Trade file corrupted")
			}
			_, _ = fmt.Sscanf(line, "%d", &trade_size[i])
			for {
				line, end = ReadLine(br)
				if end {
					break
				}
				var tr common.BLTrade
				_, _ = fmt.Sscanf(line, "%d%d%f%d", &tr.BidId, &tr.AskId, &tr.Price, &tr.Volume)
				trades[i] = append(trades[i], tr)
			}
			trade_size[i] = len(trades[i])
			tradeFile.Close()
		}
	}()
	/*
		func() {
			for i := 0; i < 2; i++ {
				orders[i] = make([]common.BLOrder, 0)
				orderFile, err := os.OpenFile(datadir+"order_"+strconv.FormatInt(int64(i+1), 10), os.O_RDONLY, 0777)
				if err != nil {
					panic("Failed to read order file")
				}
				br = bufio.NewReader(orderFile)
				line, end := ReadLine(br)
				if end {
					panic("Order file corrupted")
				}
				_, _ = fmt.Sscanf(line, "%d", &order_size[i])
				cnt := 0
				for {
					line, end = ReadLine(br)
					if end {
						break
					}
					var od common.BLOrder
					_, _ = fmt.Sscanf(line, "%d%d%d%f%d", &od.OrderId, &od.Direction, &od.Type, &od.Price, &od.Volume)
					orders[i] = append(orders[i], od)
					cnt++
				}
				orderFile.Close()
			}
			aosize = order_size[0] + order_size[1]
			ii, jj := 0, 0
			for ii < order_size[0] && jj < order_size[1] {
				if orders[0][ii].OrderId < orders[1][jj].OrderId {
					aorder = append(aorder, orders[0][ii])
					ii++
				} else {
					aorder = append(aorder, orders[1][jj])
					jj++
				}
			}
			for ; ii < order_size[0]; ii++ {
				aorder = append(aorder, orders[0][ii])
			}
			for ; jj < order_size[1]; jj++ {
				aorder = append(aorder, orders[1][jj])
			}
		}()
		func() {
			for i := 0; i < 10; i++ {
				trades[i] = make([]common.BLTrade, 0)
				tradeFile, err := os.OpenFile(datadir+"trade_"+strconv.FormatInt(int64(i+1), 10), os.O_RDONLY, 0777)
				if err != nil {
					panic("Failed to read trade file")
				}
				br = bufio.NewReader(tradeFile)
				line, end := ReadLine(br)
				if end {
					panic("Trade file corrupted")
				}
				_, _ = fmt.Sscanf(line, "%d", &trade_size[i])
				for {
					line, end = ReadLine(br)
					if end {
						break
					}
					var tr common.BLTrade
					_, _ = fmt.Sscanf(line, "%d%d%f%d", &tr.BidId, &tr.AskId, &tr.Price, &tr.Volume)
					trades[i] = append(trades[i], tr)
				}
				trade_size[i] = len(trades[i])
				tradeFile.Close()
			}
		}()
		func() {
			hookFile, err := os.OpenFile(datadir+"hook", os.O_RDONLY, 0777)
			if err != nil {
				panic("Failed to read hook file")
			}
			br = bufio.NewReader(hookFile)
			line, end := ReadLine(br)
			if end {
				panic("Hook file corrupted")
			}
			_, _ = fmt.Sscanf(line, "%d", &hook_size)
			for {
				line, end = ReadLine(br)
				if end {
					break
				}
				var hk common.BLHook
				_, _ = fmt.Sscanf(line, "%d%d%d%d", &hk.SelfOrderId, &hk.TargetStkCode, &hk.TargetTradeIdx, &hk.Arg)
				hooks = append(hooks, hk)
			}
			hookFile.Close()
		}()
		hooked := make(map[int32]bool)
		func() {
			for _, hook := range hooks {
				if trades[hook.TargetStkCode-1][hook.TargetTradeIdx-1].Volume > hook.Arg {
					hooked[hook.SelfOrderId] = true
				}
			}
		}()
	*/
	timep2 := time.Now()
	position := flag.Int("pos", 0, "The position of orders to start.")
	isdump := flag.Bool("dump", false, "Set isdump as true to activate dump at the given position.")
	isclear := flag.Bool("clear", false, "Set isclear as true to clear the cache file.")
	trade_offset := flag.Int("offset", 0, "Start from anstrade[offset] when in comparison.")
	stk_code := flag.Int("stkcode", 0, "The stock id you are matching.")
	flag.Parse()
	if *isclear {
		_ = os.Remove("./buy_cache")
		_ = os.Remove("./sell_cache")
	}
	result := make([]common.BLTrade, 0)
	func() {
		blr := new(core.BLRunner)
		blr.Load()
		if *isdump {
			for i := 0; i < aosize; i++ {
				if i == *position {
					fmt.Printf("[Dump] Succeeded in Dump,the trade offset = %d\n", len(result))
					blr.Dump()
				}
				if aorder[i].Volume == 0 {
					continue
				}
				result = append(result, blr.Dispatch(&aorder[i])...)
			}
		} else {
			for i := *position; i < aosize; i++ {
				if aorder[i].Volume == 0 {
					continue
				}
				result = append(result, blr.Dispatch(&aorder[i])...)
			}
		}
	}()
	timep3 := time.Now()
	func() {
		var i int
		fmt.Printf("AnsTrades %d,MyTrades %d\n", trade_size[*stk_code], len(result))
		tnn := trade_size[*stk_code]
		if tnn > len(result)+*trade_offset {
			tnn = len(result) + *trade_offset
		}
		for i = *trade_offset; i < tnn; i++ {
			if trades[*stk_code][i].BidId != result[i-*trade_offset].BidId {
				fmt.Printf("Differ At: %d\n", i)
				fmt.Println(trades[*stk_code][i].String())
				fmt.Println(result[i-*trade_offset].String())
				break
			}
			if trades[*stk_code][i].AskId != result[i-*trade_offset].AskId {
				fmt.Printf("Differ At: %d\n", i)
				fmt.Println(trades[*stk_code][i].String())
				fmt.Println(result[i-*trade_offset].String())
				break
			}
			if trades[*stk_code][i].Price != result[i-*trade_offset].Price {
				fmt.Printf("Differ At: %d\n", i)
				fmt.Println(trades[*stk_code][i].String())
				fmt.Println(result[i-*trade_offset].String())
				break
			}
			if trades[*stk_code][i].Volume != result[i-*trade_offset].Volume {
				fmt.Printf("Differ At: %d\n", i)
				fmt.Println(trades[*stk_code][i].String())
				fmt.Println(result[i-*trade_offset].String())
				break
			}
		}

		if i == tnn {
			fmt.Println("All Correct!")
		} else {
			fmt.Println("Wrong")
			fmt.Println(result[j].String())
			for j := 0; j < len(result); j++ {
			}
		}
	}()
	timep4 := time.Now()
	func() {
		fmt.Printf("Load data time %d ms\n", timep2.Sub(timep1).Milliseconds())
		fmt.Printf("Main time %d ms\n", timep3.Sub(timep2).Milliseconds())
		fmt.Printf("Verification time %d ms\n", timep4.Sub(timep3).Milliseconds())
	}()
	//verify
}
