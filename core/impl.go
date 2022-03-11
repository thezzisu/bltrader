package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/thezzisu/bltrader/common"
)

type ShortOrder struct {
	OrderId int32
	Price   float64
	Volume  int32
}

func (so *ShortOrder) String() string {
	return fmt.Sprintf("Short Order %d %f %d\n", so.OrderId, so.Price, so.Volume)
}

type BLRunner struct {
	buyTree    *treemap.Map
	sellTree   *treemap.Map
	buyVolume  int32
	sellVolume int32
	lowerPrice float64
	upperPrice float64
	queuePool  *sync.Pool
}

func (blrunner *BLRunner) Dispatch(order *common.BLOrder) []common.BLTrade {
	if order.Price > blrunner.upperPrice || order.Price < blrunner.lowerPrice {
		return []common.BLTrade{}
	}
	switch order.Type {
	case 0:
		return blrunner.dealLimit(order)
	case 1:
		return blrunner.oppoBest(order)
	case 2:
		return blrunner.selfBest(order)
	case 3:
		return blrunner.ins5Once(order)
	case 4:
		return blrunner.insOnce(order)
	case 5:
		return blrunner.allinOnce(order)
	}
	panic("[BLRunner Dispatch] No such order type")
}

func (blrunner *BLRunner) MyTree(order *common.BLOrder) *treemap.Map {
	if order.Direction == common.DirBuy {
		return blrunner.buyTree
	} else {
		return blrunner.sellTree
	}
}
func (blrunner *BLRunner) OtherTree(order *common.BLOrder) *treemap.Map {
	if order.Direction == common.DirBuy {
		return blrunner.sellTree
	} else {
		return blrunner.buyTree
	}
}

func (blrunner *BLRunner) GenTrade(order *common.BLOrder, ordee *ShortOrder, price float64, isMeBuy bool) common.BLTrade {
	vol := order.Volume
	if vol > ordee.Volume {
		vol = ordee.Volume
	}
	if isMeBuy {
		blrunner.sellVolume -= vol
		return common.BLTrade{StkCode: order.StkCode, BidId: order.OrderId, AskId: ordee.OrderId, Price: price, Volume: vol}
	} else {
		blrunner.buyVolume -= vol
		return common.BLTrade{StkCode: order.StkCode, BidId: ordee.OrderId, AskId: order.OrderId, Price: price, Volume: vol}
	}
}

func (blrunner *BLRunner) dealLimit(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.OtherTree(order)
	isMeBuy := order.Direction == common.DirBuy
	trades := make([]common.BLTrade, 0, 1)
	shorter := ShortOrder{order.OrderId, order.Price, order.Volume}
	bound := order.Price
	it := rbt.Iterator()
	for it.Next() {
		if order.Volume == 0 {
			break
		}
		oprice, q := it.Key().(float64), it.Value().(*Queue)
		if isMeBuy {
			if oprice > bound {
				break
			}
		} else {
			if oprice < bound {
				break
			}
		}
		u := q.head
		endFlag := false
		for ; u != nil && !endFlag; u = q.head {
			if u.order.Volume <= order.Volume {
				trades = append(trades, blrunner.GenTrade(order, &u.order, u.order.Price, isMeBuy))
				order.Volume -= u.order.Volume
				q.Free(blrunner.queuePool)
				if order.Volume == 0 {
					break
				}
			} else {
				trades = append(trades, blrunner.GenTrade(order, &u.order, u.order.Price, isMeBuy))
				u.order.Volume -= order.Volume
				order.Volume = 0
				endFlag = true
			}
		}
		if q.head == nil {
			rbt.Remove(oprice)
		}
	}
	if order.Volume > 0 {
		shorter.Volume = order.Volume
		blrunner.InsertOrder(blrunner.MyTree(order), &shorter)
		if isMeBuy {
			blrunner.buyVolume += shorter.Volume
		} else {
			blrunner.sellVolume += shorter.Volume
		}
	}
	return trades
}

func (blrunner *BLRunner) oppoBest(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.OtherTree(order)
	isMeBuy := order.Direction == common.DirBuy
	trades := make([]common.BLTrade, 0, 1)
	if isMeBuy && blrunner.sellVolume == 0 || !isMeBuy && blrunner.buyVolume == 0 {
		return trades
	}
	shorter := ShortOrder{order.OrderId, order.Price, order.Volume}
	var q *Queue
	a_, b_ := rbt.Min()
	if a_ == nil {
		return trades
	}
	shorter.Price, q = a_.(float64), b_.(*Queue)
	u := q.head
	endFlag := false
	for ; u != nil && !endFlag; u = q.head {
		if u.order.Volume <= order.Volume {
			trades = append(trades, blrunner.GenTrade(order, &u.order, u.order.Price, isMeBuy))
			order.Volume -= u.order.Volume
			q.Free(blrunner.queuePool)
			if order.Volume == 0 {
				break
			}
		} else {
			trades = append(trades, blrunner.GenTrade(order, &u.order, u.order.Price, isMeBuy))
			u.order.Volume -= order.Volume
			order.Volume = 0
			endFlag = true
		}
	}
	if q.head == nil {
		rbt.Remove(shorter.Price)
	}
	if order.Volume > 0 {
		shorter.Volume = order.Volume
		blrunner.InsertOrder(blrunner.MyTree(order), &shorter)
		if isMeBuy {
			blrunner.buyVolume += shorter.Volume
		} else {
			blrunner.sellVolume += shorter.Volume
		}
	}
	return trades
}

func (blrunner *BLRunner) selfBest(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.MyTree(order)
	isMeBuy := order.Direction == common.DirBuy
	trades := make([]common.BLTrade, 0, 1)
	if isMeBuy && blrunner.buyVolume == 0 || !isMeBuy && blrunner.sellVolume == 0 {
		return trades
	}
	shorter := ShortOrder{order.OrderId, order.Price, order.Volume}
	var q *Queue
	a_, b_ := rbt.Min()
	if a_ == nil {
		return trades
	}
	shorter.Price, q = a_.(float64), b_.(*Queue)
	if isMeBuy {
		blrunner.buyVolume += shorter.Volume
	} else {
		blrunner.sellVolume += shorter.Volume
	}
	q.Push(blrunner.queuePool, &shorter)
	return trades
}

func (blrunner *BLRunner) insOnce(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.OtherTree(order)
	isMeBuy := order.Direction == common.DirBuy
	trades := make([]common.BLTrade, 0, 1)
	it := rbt.Iterator()
	for it.Next() {
		if order.Volume == 0 {
			break
		}
		oprice, q := it.Key().(float64), it.Value().(*Queue)
		u := q.head
		endFlag := false
		for ; u != nil && (!endFlag); u = q.head {
			if u.order.Volume <= order.Volume {
				trades = append(trades, blrunner.GenTrade(order, &u.order, u.order.Price, isMeBuy))
				order.Volume -= u.order.Volume
				q.Free(blrunner.queuePool)
				if order.Volume == 0 {
					break
				}
			} else {
				trades = append(trades, blrunner.GenTrade(order, &u.order, u.order.Price, isMeBuy))
				u.order.Volume -= order.Volume
				order.Volume = 0
				endFlag = true
			}
		}
		if q.head == nil {
			rbt.Remove(oprice)
		}
	}
	return trades
}

func (blrunner *BLRunner) ins5Once(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.OtherTree(order)
	isMeBuy := order.Direction == common.DirBuy
	trades := make([]common.BLTrade, 0, 1)
	it := rbt.Iterator()
	k := 0
	for it.Next() {
		if order.Volume == 0 {
			break
		}
		oprice, q := it.Key().(float64), it.Value().(*Queue)
		u := q.head
		endFlag := false
		for ; u != nil && !endFlag; u = q.head {
			if u.order.Volume <= order.Volume {
				trades = append(trades, blrunner.GenTrade(order, &u.order, u.order.Price, isMeBuy))
				order.Volume -= u.order.Volume
				q.Free(blrunner.queuePool)
				if order.Volume == 0 {
					break
				}
			} else {
				trades = append(trades, blrunner.GenTrade(order, &u.order, u.order.Price, isMeBuy))
				u.order.Volume -= order.Volume
				order.Volume = 0
				endFlag = true
			}
		}
		if q.head == nil {
			rbt.Remove(oprice)
		}
		if k++; k >= 5 {
			break
		}
	}
	return trades
}

func (blrunner *BLRunner) allinOnce(order *common.BLOrder) []common.BLTrade {
	if order.Direction == common.DirBuy {
		if order.Volume <= blrunner.sellVolume {
			return blrunner.insOnce(order)
		} else {
			return []common.BLTrade{}
		}
	} else {
		if order.Volume <= blrunner.buyVolume {
			return blrunner.insOnce(order)
		} else {
			return []common.BLTrade{}
		}
	}
}

func byPriceAscend(a, b interface{}) int {
	c1 := a.(float64)
	c2 := b.(float64)
	switch {
	case c1 > c2:
		return 1
	case c1 < c2:
		return -1
	default:
		return 0
	}
}
func byPriceDescend(a, b interface{}) int {
	c1 := a.(float64)
	c2 := b.(float64)
	switch {
	case c1 < c2:
		return 1
	case c1 > c2:
		return -1
	default:
		return 0
	}
}

func (blrunner *BLRunner) InsertOrder(rbt *treemap.Map, order *ShortOrder) {
	if q_, ok := rbt.Get(order.Price); !ok {
		q := new(Queue)
		rbt.Put(order.Price, q)
		q.Push(blrunner.queuePool, order)
	} else {
		q := q_.(*Queue)
		q.Push(blrunner.queuePool, order)
	}
}

func (so *ShortOrder) Encode() []byte {
	buf := bytes.NewBuffer(nil)
	err1 := binary.Write(buf, binary.LittleEndian, so.OrderId)
	err2 := binary.Write(buf, binary.LittleEndian, so.Price)
	err3 := binary.Write(buf, binary.LittleEndian, so.Volume)
	if err1 != nil || err2 != nil || err3 != nil {
		return nil
	}
	return buf.Bytes()
}

func (so *ShortOrder) Decode(data []byte) {
	buf := bytes.NewBuffer(data)
	_ = binary.Read(buf, binary.LittleEndian, &so.OrderId)
	_ = binary.Read(buf, binary.LittleEndian, &so.Price)
	_ = binary.Read(buf, binary.LittleEndian, &so.Volume)
}

func (so *ShortOrder) Read(rFile *os.File) bool {
	// return true if file reaches eof
	buf := make([]byte, 16)
	if _, err := rFile.Read(buf); err != nil {
		return true
	}
	rb := bytes.NewReader(buf)
	_, _ = rb.Read(buf)
	so.Decode(buf)
	return false
}

func (so *ShortOrder) Write(wFile *os.File) {
	_, _ = wFile.Write(so.Encode())
}

/*

Write the _Volume in the first order
order.Volume
order.OrderId = order.Price = 0

*/

func (blrunner *BLRunner) Load(lower float64, upper float64) {
	blrunner.buyTree = treemap.NewWith(byPriceDescend)
	blrunner.sellTree = treemap.NewWith(byPriceAscend)
	blrunner.lowerPrice = lower
	blrunner.upperPrice = upper
	blrunner.queuePool = &sync.Pool{New: func() interface{} { return new(LinkNode) }}

	blrunner.buyVolume, blrunner.sellVolume = 0, 0
	return
	// Disable load cache

	_, errB := os.Stat("./buy_cache")
	_, errS := os.Stat("./sell_cache")
	noB, noS := os.IsNotExist(errB), os.IsNotExist(errS)
	if noB || noS {
		blrunner.buyVolume, blrunner.sellVolume = 0, 0
		return
	}
	bFile, _ := os.OpenFile("./buy_cache", os.O_RDONLY, 0777)
	sFile, _ := os.OpenFile("./sell_cache", os.O_RDONLY, 0777)

	ReadCache := func(rFile *os.File, sVolume *int32, tree *treemap.Map) {
		var order ShortOrder
		end := order.Read(rFile)
		if end {
			return
		}
		*sVolume = order.Volume
		fmt.Printf("sVolume %d\n", *sVolume)
		for {
			end = order.Read(rFile)
			if end {
				return
			}
			blrunner.InsertOrder(tree, &order)
		}
	}
	ReadCache(bFile, &blrunner.buyVolume, blrunner.buyTree)
	ReadCache(sFile, &blrunner.sellVolume, blrunner.sellTree)

	bFile.Close()
	sFile.Close()
}

func (blrunner *BLRunner) Dump() {
	bFile, errB := os.OpenFile("./buy_cache", os.O_CREATE|os.O_WRONLY, 0777)
	sFile, errS := os.OpenFile("./sell_cache", os.O_CREATE|os.O_WRONLY, 0777)
	if errB != nil || errS != nil {
		panic("Failed to write cache")
	}

	fmt.Printf("[Dump] buy %d sell %d\n", blrunner.buyVolume, blrunner.sellVolume) // debug output

	WriteCache := func(wFile *os.File, sVolume int32, tree *treemap.Map) {
		var order ShortOrder
		order.Volume = sVolume
		order.Write(wFile)
		it := tree.Iterator()
		for it.Next() {
			u := it.Value().(*Queue).head
			for ; u != nil; u = u.next {
				u.order.Write(wFile)
			}
		}
	}
	WriteCache(bFile, blrunner.buyVolume, blrunner.buyTree)
	WriteCache(sFile, blrunner.sellVolume, blrunner.sellTree)

	bFile.Close()
	sFile.Close()
}
