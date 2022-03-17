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

const SoSize = 10

type ShortOrder struct {
	OrderId int32
	Price   int32
	Volume  int16
}

type BLRunner struct {
	buyTree    *treemap.Map
	sellTree   *treemap.Map
	buyVolume  int64
	sellVolume int64
	queuePool  *sync.Pool
}

func (blrunner *BLRunner) Dispatch(order *common.BLOrder) []common.BLTradeComp {
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

func (blrunner *BLRunner) GenTrade(order *ShortOrder, oid int32, ovo int16, price int32, isMeBuy bool) common.BLTradeComp {
	vol := int16(order.Volume)
	if vol > ovo {
		vol = ovo
	}
	if isMeBuy {
		blrunner.sellVolume -= int64(vol)
		return common.BLTradeComp{BidId: order.OrderId, AskId: oid, Price: price, Volume: vol}
	} else {
		blrunner.buyVolume -= int64(vol)
		return common.BLTradeComp{BidId: oid, AskId: order.OrderId, Price: price, Volume: vol}
	}
}

func (blrunner *BLRunner) dealLimit(order *common.BLOrder) []common.BLTradeComp {
	rbt := blrunner.OtherTree(order)
	isMeBuy := order.Direction == common.DirBuy
	trades := make([]common.BLTradeComp, 0, 1)
	shorter := ShortOrder{order.OrderId, common.PriceF2I(order.Price), int16(order.Volume)}
	bound := common.PriceF2I(order.Price)
	it := rbt.Iterator()
	for it.Next() {
		if shorter.Volume == 0 {
			break
		}
		oprice, q := it.Key().(int32), it.Value().(*Queue)
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
			if u.Volume <= shorter.Volume {
				trades = append(trades, blrunner.GenTrade(&shorter, u.OrderId, u.Volume, oprice, isMeBuy))
				shorter.Volume -= u.Volume
				q.Free(blrunner.queuePool)
				if shorter.Volume == 0 {
					break
				}
			} else {
				trades = append(trades, blrunner.GenTrade(&shorter, u.OrderId, u.Volume, oprice, isMeBuy))
				u.Volume -= shorter.Volume
				shorter.Volume = 0
				endFlag = true
			}
		}
		if q.head == nil {
			rbt.Remove(oprice)
		}
	}
	if shorter.Volume > 0 {
		blrunner.InsertOrder(blrunner.MyTree(order), &shorter)
		if isMeBuy {
			blrunner.buyVolume += int64(shorter.Volume)
		} else {
			blrunner.sellVolume += int64(shorter.Volume)
		}
	}
	return trades
}

func (blrunner *BLRunner) oppoBest(order *common.BLOrder) []common.BLTradeComp {
	rbt := blrunner.OtherTree(order)
	isMeBuy := order.Direction == common.DirBuy
	trades := make([]common.BLTradeComp, 0, 1)
	if isMeBuy && blrunner.sellVolume == 0 || !isMeBuy && blrunner.buyVolume == 0 {
		return trades
	}
	shorter := ShortOrder{order.OrderId, common.PriceF2I(order.Price), int16(order.Volume)}
	var q *Queue
	a_, b_ := rbt.Min()
	if a_ == nil {
		return trades
	}
	shorter.Price, q = a_.(int32), b_.(*Queue)
	u := q.head
	endFlag := false
	for ; u != nil && !endFlag; u = q.head {
		if u.Volume <= shorter.Volume {
			trades = append(trades, blrunner.GenTrade(&shorter, u.OrderId, u.Volume, shorter.Price, isMeBuy))
			shorter.Volume -= u.Volume
			q.Free(blrunner.queuePool)
			if shorter.Volume == 0 {
				break
			}
		} else {
			trades = append(trades, blrunner.GenTrade(&shorter, u.OrderId, u.Volume, shorter.Price, isMeBuy))
			u.Volume -= shorter.Volume
			shorter.Volume = 0
			endFlag = true
		}
	}
	if q.head == nil {
		rbt.Remove(shorter.Price)
	}
	if shorter.Volume > 0 {
		blrunner.InsertOrder(blrunner.MyTree(order), &shorter)
		if isMeBuy {
			blrunner.buyVolume += int64(shorter.Volume)
		} else {
			blrunner.sellVolume += int64(shorter.Volume)
		}
	}
	return trades
}

func (blrunner *BLRunner) selfBest(order *common.BLOrder) []common.BLTradeComp {
	rbt := blrunner.MyTree(order)
	isMeBuy := order.Direction == common.DirBuy
	trades := make([]common.BLTradeComp, 0, 1)
	if isMeBuy && blrunner.buyVolume == 0 || !isMeBuy && blrunner.sellVolume == 0 {
		return trades
	}
	shorter := ShortOrder{order.OrderId, common.PriceF2I(order.Price), int16(order.Volume)}
	var q *Queue
	a_, b_ := rbt.Min()
	if a_ == nil {
		return trades
	}
	shorter.Price, q = a_.(int32), b_.(*Queue)
	if isMeBuy {
		blrunner.buyVolume += int64(shorter.Volume)
	} else {
		blrunner.sellVolume += int64(shorter.Volume)
	}
	q.Push(blrunner.queuePool, shorter.OrderId, shorter.Volume)
	return trades
}

func (blrunner *BLRunner) insOnce(order *common.BLOrder) []common.BLTradeComp {
	rbt := blrunner.OtherTree(order)
	isMeBuy := order.Direction == common.DirBuy
	trades := make([]common.BLTradeComp, 0, 1)
	it := rbt.Iterator()
	shorter := ShortOrder{order.OrderId, common.PriceF2I(order.Price), int16(order.Volume)}
	for it.Next() {
		if shorter.Volume == 0 {
			break
		}
		oprice, q := it.Key().(int32), it.Value().(*Queue)
		u := q.head
		endFlag := false
		for ; u != nil && (!endFlag); u = q.head {
			if u.Volume <= shorter.Volume {
				trades = append(trades, blrunner.GenTrade(&shorter, u.OrderId, u.Volume, oprice, isMeBuy))
				shorter.Volume -= u.Volume
				q.Free(blrunner.queuePool)
				if shorter.Volume == 0 {
					break
				}
			} else {
				trades = append(trades, blrunner.GenTrade(&shorter, u.OrderId, u.Volume, oprice, isMeBuy))
				u.Volume -= shorter.Volume
				shorter.Volume = 0
				endFlag = true
			}
		}
		if q.head == nil {
			rbt.Remove(oprice)
		}
	}
	return trades
}

func (blrunner *BLRunner) ins5Once(order *common.BLOrder) []common.BLTradeComp {
	rbt := blrunner.OtherTree(order)
	isMeBuy := order.Direction == common.DirBuy
	trades := make([]common.BLTradeComp, 0, 1)
	it := rbt.Iterator()
	k := 0
	shorter := ShortOrder{order.OrderId, common.PriceF2I(order.Price), int16(order.Volume)}
	for it.Next() {
		if shorter.Volume == 0 {
			break
		}
		oprice, q := it.Key().(int32), it.Value().(*Queue)
		u := q.head
		endFlag := false
		for ; u != nil && !endFlag; u = q.head {
			if u.Volume <= shorter.Volume {
				trades = append(trades, blrunner.GenTrade(&shorter, u.OrderId, u.Volume, oprice, isMeBuy))
				shorter.Volume -= u.Volume
				q.Free(blrunner.queuePool)
				if shorter.Volume == 0 {
					break
				}
			} else {
				trades = append(trades, blrunner.GenTrade(&shorter, u.OrderId, u.Volume, oprice, isMeBuy))
				u.Volume -= shorter.Volume
				shorter.Volume = 0
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

func (blrunner *BLRunner) allinOnce(order *common.BLOrder) []common.BLTradeComp {
	if order.Direction == common.DirBuy {
		if int64(order.Volume) <= blrunner.sellVolume {
			return blrunner.insOnce(order)
		} else {
			return []common.BLTradeComp{}
		}
	} else {
		if int64(order.Volume) <= blrunner.buyVolume {
			return blrunner.insOnce(order)
		} else {
			return []common.BLTradeComp{}
		}
	}
}

func byPriceAscend(a, b interface{}) int {
	c1 := a.(int32)
	c2 := b.(int32)
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
	c1 := a.(int32)
	c2 := b.(int32)
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
		q.Push(blrunner.queuePool, order.OrderId, order.Volume)
	} else {
		q := q_.(*Queue)
		q.Push(blrunner.queuePool, order.OrderId, order.Volume)
	}
}

const Cksize = 65536

type FChunk struct {
	buf  *bytes.Buffer
	n    int
	file *os.File
}

func (fc *FChunk) Bind(f *os.File) {
	fc.buf = bytes.NewBuffer(nil)
	fc.buf.Grow(Cksize)
	fc.file = f
}

func (fc *FChunk) Read() *bytes.Buffer {
	if fc.n == 0 {
		rb := make([]byte, Cksize)
		if fc.n, _ = fc.file.Read(rb); fc.n <= 0 {
			return nil
		}
		fc.buf = bytes.NewBuffer(rb)
	}
	fc.n -= SoSize
	return fc.buf
}

func (fc *FChunk) Write() *bytes.Buffer {
	if fc.n == Cksize {
		fc.file.Write(fc.buf.Bytes())
		fc.buf = bytes.NewBuffer(nil)
		fc.n = 0
	}
	fc.n += SoSize
	return fc.buf
}

func (fc *FChunk) WFlush() {
	fc.file.Write(fc.buf.Bytes())
}

func (so *ShortOrder) Encode() []byte {
	buf := bytes.NewBuffer(nil)
	_ = binary.Write(buf, binary.LittleEndian, so.OrderId)
	_ = binary.Write(buf, binary.LittleEndian, so.Price)
	_ = binary.Write(buf, binary.LittleEndian, so.Volume)
	return buf.Bytes()
}

func (so *ShortOrder) Read(chunk *FChunk) bool {
	// return true if file reaches eof
	buf := chunk.Read()
	if buf == nil {
		return true
	}
	_ = binary.Read(buf, binary.LittleEndian, &so.OrderId)
	_ = binary.Read(buf, binary.LittleEndian, &so.Price)
	_ = binary.Read(buf, binary.LittleEndian, &so.Volume)
	return false
}

func (so *ShortOrder) Write(chunk *FChunk) {
	buf := chunk.Write()
	_ = binary.Write(buf, binary.LittleEndian, so.OrderId)
	_ = binary.Write(buf, binary.LittleEndian, so.Price)
	_ = binary.Write(buf, binary.LittleEndian, so.Volume)
}

/*

TODO refactor these code

Write the _Volume in the first order
order.Volume
order.OrderId = order.Price = 0

*/

func (blrunner *BLRunner) Load(bid string) {
	blrunner.buyTree = treemap.NewWith(byPriceDescend)
	blrunner.sellTree = treemap.NewWith(byPriceAscend)
	blrunner.queuePool = &sync.Pool{New: func() interface{} { return new(LinkNode) }}

	//blrunner.buyVolume, blrunner.sellVolume = 0, 0
	//return
	// Disable load cache

	_, errB := os.Stat("./buy_cache" + bid)
	_, errS := os.Stat("./sell_cache" + bid)
	noB, noS := os.IsNotExist(errB), os.IsNotExist(errS)
	if noB || noS {
		blrunner.buyVolume, blrunner.sellVolume = 0, 0
		return
	}
	bFile, _ := os.OpenFile("./buy_cache"+bid, os.O_RDONLY, 0600)
	sFile, _ := os.OpenFile("./sell_cache"+bid, os.O_RDONLY, 0600)

	ReadCache := func(rFile *os.File, sVolume *int64, tree *treemap.Map) {
		var order ShortOrder
		chunk := new(FChunk)
		chunk.Bind(rFile)
		end := order.Read(chunk)
		if end {
			return
		}
		*sVolume = int64(order.Price)*int64(1000000000) + int64(order.OrderId)
		fmt.Printf("sVolume %d\n", *sVolume)
		for {
			end = order.Read(chunk)
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

func (blrunner *BLRunner) Dump(bid string) {
	bFile, errB := os.OpenFile("./buy_cache"+bid, os.O_CREATE|os.O_WRONLY, 0600)
	sFile, errS := os.OpenFile("./sell_cache"+bid, os.O_CREATE|os.O_WRONLY, 0600)
	if errB != nil || errS != nil {
		panic("Failed to write cache")
	}

	WriteCache := func(wFile *os.File, sVolume int64, tree *treemap.Map) {
		chunk := new(FChunk)
		chunk.Bind(wFile)
		var order ShortOrder
		// TODO fixme
		order.Price = int32(sVolume / int64(1000000000))
		order.OrderId = int32(sVolume % int64(1000000000))
		order.Write(chunk)
		it := tree.Iterator()
		for it.Next() {
			tprice, u := it.Key().(int32), it.Value().(*Queue).head
			for ; u != nil; u = u.next {
				order.OrderId = u.OrderId
				order.Volume = u.Volume
				order.Price = tprice
				order.Write(chunk)
			}
		}
		chunk.WFlush()
	}
	WriteCache(bFile, blrunner.buyVolume, blrunner.buyTree)
	WriteCache(sFile, blrunner.sellVolume, blrunner.sellTree)

	bFile.Close()
	sFile.Close()
}
