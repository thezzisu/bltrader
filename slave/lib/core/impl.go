package core

import (
	"bytes"
	"encoding/binary"
	"os"

	arbt "github.com/ocdogan/rbt"
	"github.com/thezzisu/bltrader/common"
)

type BLRunner struct {
	BuyTree    *arbt.RbTree
	SellTree   *arbt.RbTree
	BuyVolume  int
	SellVolume int
	QueueSlab  *Chunk
	LowerPrice float64
	UpperPrice float64
}

func (blrunner *BLRunner) Dispatch(order *common.BLOrder) []common.BLTrade {
	if order.Price > blrunner.UpperPrice || order.Price < blrunner.LowerPrice {
		return []common.BLTrade{}
	} //静态上下界
	if order.Type == common.DirBuy {
		order.Price = -order.Price
	} //
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

func (blrunner *BLRunner) MyTree(order *common.BLOrder) *arbt.RbTree {
	if order.Direction == common.DirBuy {
		return blrunner.BuyTree
	} else {
		return blrunner.SellTree
	}
}
func (blrunner *BLRunner) OtherTree(order *common.BLOrder) *arbt.RbTree {
	if order.Direction == common.DirSell {
		return blrunner.BuyTree
	} else {
		return blrunner.SellTree
	}
}

//ordee树上挂着的 order主动方 checked
func GenTrade(order *common.BLOrder, ordee *common.BLOrder, price float64, isBuy bool) common.BLTrade {
	var vol int
	if order.Volume > ordee.Volume {
		vol = ordee.Volume
	} else {
		vol = order.Volume
	}
	if isBuy {
		return common.BLTrade{StkCode: order.StkCode, BidId: order.OrderId, AskId: ordee.OrderId, Price: -price, Volume: vol}
	} else {
		return common.BLTrade{StkCode: order.StkCode, BidId: ordee.OrderId, AskId: order.OrderId, Price: price, Volume: vol}
	}
}

func (blrunner *BLRunner) dealLimit(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.OtherTree(order) //对方树指针
	isMeBuy := rbt == blrunner.SellTree
	bound := arbt.Float64Key(-order.Price)
	trades := make([]common.BLTrade, 0, 1)
	iterator, _ := rbt.NewRbIterator(func(iterator arbt.RbIterator, key arbt.RbKey, value interface{}) {
		u := value.head
		for ; u != nil && !iterator.Closed(); u = value.head {
			if u.order.Volume <= order.Volume {
				trades = append(trades, GenTrade(order, u.order, -u.order.Price, isMeBuy))
				order.Volume -= u.order.Volume
				value.Free(blrunner.QueueSlab)
			} else {
				trades = append(trades, GenTrade(order, u.order, -u.order.Price, isMeBuy))
				u.order.Volume -= order.Volume
				order.Volume = 0
				iterator.Close()
			}
		}
		if value.head == nil {
			rbt.Delete(key)
		}
	})
	iterator.LessOrEqual(&bound)
	if order.Volume > 0 {
		blrunner.MyTree(order).InsertOrder(order)
		if isMeBuy {
			blrunner.BuyVolume += order.Volume
		} else {
			blrunner.SellVolume += order.Volume
		}
	}
	return trades
}

func (blrunner *BLRunner) oppoBest(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.OtherTree(order)
	isMeBuy := rbt == blrunner.SellTree //我方是否是买方
	trades := make([]common.BLTrade, 0, 1)
	if isMeBuy && blrunner.SellVolume == 0 || !isMeBuy && blrunner.BuyVolume == 0 {
		return trades
	}
	//修改
	//大改

	price, _ := rbt.Min()
	order.Price = -float64(*price.(*arbt.Float64Key))
	return blrunner.dealLimit(order)
}

func (blrunner *BLRunner) selfBest(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.MyTree(order)
	isMeBuy := rbt == blrunner.BuyTree
	trades := make([]common.BLTrade, 0, 1)
	if isMeBuy && blrunner.BuyVolume == 0 || !isMeBuy && blrunner.SellVolume == 0 {
		return trades
	}
	price, _ := rbt.Max()
	order.Price = float64(*price.(*arbt.Float64Key)) //类型不匹配
	return blrunner.dealLimit(order)
}

func (blrunner *BLRunner) insOnce(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.OtherTree(order)
	isMeBuy := rbt == blrunner.SellTree
	trades := make([]common.BLTrade, 0, 1)
	iterator, _ := rbt.NewRbIterator(func(iterator arbt.RbIterator, key arbt.RbKey, value interface{}) {
		u := value.head
		for ; u != nil && !iterator.Closed(); u = value.head {
			if u.order.Volume <= order.Volume {
				trades = append(trades, GenTrade(order, u.order, -u.order.price, isMeBuy))
				order.Volume -= u.order.Volume
				value.Free(blrunner.QueueSlab)
			} else {
				trades = append(trades, GenTrade(order, u.order, -u.order.price, isMeBuy))
				u.order.Volume -= order.Volume
				order.Volume = 0
				iterator.Close()
			}
		}
		if value.head == nil {
			rbt.Delete(key)
		}
	})
	iterator.All()
	return trades
}

func (blrunner *BLRunner) ins5Once(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.OtherTree(order)
	isMeBuy := rbt == blrunner.SellTree
	trades := make([]common.BLTrade, 0, 1)
	k := 0
	iterator, _ := rbt.NewRbIterator(func(iterator arbt.RbIterator, key arbt.RbKey, value interface{}) {
		u := value.head
		for ; u != nil && !iterator.Closed(); u = value.head {
			if u.order.Volume <= order.Volume {
				trades = append(trades, GenTrade(order, u.order, -u.order.price, isMeBuy))
				order.Volume -= u.order.Volume
				value.Free(blrunner.QueueSlab)
			} else {
				trades = append(trades, GenTrade(order, u.order, -u.order.price, isMeBuy))
				u.order.Volume -= order.Volume
				order.Volume = 0
				iterator.Close()
			}
		}
		if value.head == nil {
			rbt.Delete(key)
		}
		if k++; k >= 5 {
			iterator.Close()
		}
	})
	iterator.All()
	return trades
}

func (blrunner *BLRunner) allinOnce(order *common.BLOrder) []common.BLTrade {
	if order.Direction == common.DirBuy {
		if order.Volume <= int(blrunner.SellVolume) {
			return blrunner.insOnce(order)
		} else {
			return []common.BLTrade{}
		}
	} else {
		if order.Volume <= int(blrunner.BuyVolume) {
			return blrunner.insOnce(order)
		} else {
			return []common.BLTrade{}
		}
	}
}

func (rbt *arbt.RbTree) InsertOrder(order *common.BLOrder) {
	price := arbt.Float64Key(order.Price)
	if node, ok := rbt.Get(&price); ok {
		node.Push(blrunner.Chunk, order)
	} else {
		node = new(Queue)
		node.Push(blrunner.Chunk, order)
		rbt.Insert(&price, node)
	}
}

func (blrunner *BLRunner) Load(lower float64, upper float64) {
	blrunner.BuyTree = arbt.NewRbTree()
	blrunner.SellTree = arbt.NewRbTree()
	blrunner.LowerPrice = lower
	blrunner.UpperPrice = upper
	const ChunkSize = 1048576
	blrunner.QueueSlab = NewChunk(ChunkSize)
	buyFile, errB := os.OpenFile("./buy_cache", os.O_RDONLY, 0777)
	sellFile, errS := os.OpenFile("./sell_cache", os.O_RDONLY, 0777)
	if errB != nil || errS != nil {
		blrunner.BuyVolume, blrunner.SellVolume = 0, 0
		if errB == nil {
			buyFile.Close()
		}
		if errS == nil {
			sellFile.Close()
		}
		return
	}
	buf := make([]byte, 8)
	if _, err := buyFile.Read(buf); err == nil {
		rbuf := bytes.NewReader(buf)
		binary.Read(rbuf, binary.LittleEndian, &blrunner.BuyVolume)
		//blrunner.BuyVolume = int64(buf)
	} else {
		panic("Cache corrupt")
	}
	if _, err := sellFile.Read(buf); err == nil {
		rbuf := bytes.NewReader(buf)
		binary.Read(rbuf, binary.LittleEndian, &blrunner.SellVolume)
		//blrunner.SellVolume = int64(buf)
	} else {
		panic("Cache corrupt")
	}
	parse := func(data []byte) *common.BLOrder {
		rbuf := bytes.NewReader(data)
		order := new(common.BLOrder)
		binary.Read(rbuf, binary.LittleEndian, &order.StkCode)
		binary.Read(rbuf, binary.LittleEndian, &order.OrderId)
		binary.Read(rbuf, binary.LittleEndian, &order.Direction)
		binary.Read(rbuf, binary.LittleEndian, &order.Type)
		binary.Read(rbuf, binary.LittleEndian, &order.Price)
		binary.Read(rbuf, binary.LittleEndian, &order.Volume)
		return order
	}
	buf = make([]byte, 28)
	for {
		if _, err := buyFile.Read(buf); err == nil {
			blrunner.BuyTree.InsertOrder(parse(buf))
		} else {
			break
		}
	}
	for {
		if _, err := sellFile.Read(buf); err == nil {
			blrunner.SellTree.InsertOrder(parse(buf))
		} else {
			break
		}
	}
	buyFile.Close()
	sellFile.Close()
}

func (blrunner *BLRunner) Dump() {
	buyFile, errB := os.OpenFile("./buy_cache", os.O_WRONLY, 0777)
	sellFile, errS := os.OpenFile("./sell_cache", os.O_WRONLY, 0777)
	if errB != nil || errS != nil {
		panic("Failed to write cache")
	}
	wbuf := new(bytes.Buffer)
	_ = binary.Write(wbuf, binary.LittleEndian, blrunner.BuyVolume)
	_, _ = buyFile.Write(wbuf.Bytes())
	wbuf = new(bytes.Buffer)
	_ = binary.Write(wbuf, binary.LittleEndian, blrunner.SellVolume)
	_, _ = sellFile.Write(wbuf.Bytes())
	iterator, _ := blrunner.BuyTree.NewRbIterator(func(iterator arbt.RbIterator, key arbt.RbKey, value interface{}) {
		u := value.head
		for ; u != nil; u = u.next {
			wbuf := new(bytes.Buffer)
			_ = binary.Write(wbuf, binary.LittleEndian, u.order.OrderId)
			_ = binary.Write(wbuf, binary.LittleEndian, u.order.StkCode)
			_ = binary.Write(wbuf, binary.LittleEndian, u.order.Direction)
			_ = binary.Write(wbuf, binary.LittleEndian, u.order.Type)
			_ = binary.Write(wbuf, binary.LittleEndian, u.order.Price)
			_ = binary.Write(wbuf, binary.LittleEndian, u.order.Volume)
			_, _ = buyFile.Write(wbuf.Bytes())
		}
	})
	iterator.All()
	iterator, _ = blrunner.SellTree.NewRbIterator(func(iterator arbt.RbIterator, key arbt.RbKey, value interface{}) {
		u := value.head
		for ; u != nil; u = u.next {
			wbuf = new(bytes.Buffer)
			_ = binary.Write(wbuf, binary.LittleEndian, u.order.StkCode)
			_ = binary.Write(wbuf, binary.LittleEndian, u.order.OrderId)
			_ = binary.Write(wbuf, binary.LittleEndian, u.order.Direction)
			_ = binary.Write(wbuf, binary.LittleEndian, u.order.Type)
			_ = binary.Write(wbuf, binary.LittleEndian, u.order.Price)
			_ = binary.Write(wbuf, binary.LittleEndian, u.order.Volume)
			_, _ = sellFile.Write(wbuf.Bytes())
		}
	})
	iterator.All()
	buyFile.Close()
	sellFile.Close()
}

//load and dump 64->int
