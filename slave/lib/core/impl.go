package core

import (
	arbt "github.com/octogan/rbt"
	"encoding/binary"
	"github.com/thezzisu/bltrader/common"
	"bytes"
	"fmt"
	"io"
	"os"
)



type BLRunner struct {
	BuyTree    *arbt.RbTree
	SellTree   *arbt.RbTree
	BuyVolume  uint64 	
	SellVolume uint64
	QueueSlab	*Chunk
	LowerPrice	float64
	UpperPrice	float64
}

func (blrunner *BLRunner) Dispatch(order *common.BLOrder) []common.BLTrade {
	if order.Price > blrunner.UpperPrice || order.Price < blrunner.LowerPrice {
		return []common.BLTrade{}
	}
	if order.Type == common.DirBuy {
		order.Price = -order.Price
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

func (blrunner *BLRunner) MyTree(order *common.BLOrder) *arbt.RbTree {
	if(order.Type == common.DirBuy){
		return blrunner.BuyTree
	}else{
		return blrunner.SellTree
	}
}
func (blrunner *BLRunner) OtherTree(order *common.BLOrder) *arbt.RbTree {
	if(order.Type == common.DirBuy){
		return blrunner.BuyTree
	}else{
		return blrunner.SellTree
	}
}


func GenTrade(order *common.BLOrder,ordee *common.BLOrder,price Float64,isBuy bool) common.BLTrade {
	vol := order.Volume - ordee.Volume
	if(vol < 0){
		vol = -vol
	}
	if isBuy{
		return common.BLTrade{StkCode:order.StkCode,BidId:order.OrderId,AskId:ordee.OrderId,Price:-price,Volume:vol}
	}else{
		return common.BLTrade{StkCode:order.StkCode,BidId:ordee.OrderId,AskId:order.OrderId,Price:price,Volume:vol}
	}
}

func (blrunner *BLRunner) dealLimit(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.OtherTree(order)
	isMeBuy := rbt == blrunner.SellTree
	bound := Float64Key(-order.Price)
	trades := make([]common.BLTrade,0,1)
	iterator, err := rbt.NewRbIterator(func(iterator RbIterator, key RbKey, value interface{}){
        u := value.head
		for ;u != nil && !iterator.Closed();u = value.head{
			if(u.order.Volume <= order.Volume){
				trades = append(trades,GenTrade(order,u.order,-u.order.price,isMeBuy))
				order.Volume -= u.order.Volume
				value.Pop(blrunner.QueueSlab)
			}else{
				trades = append(trades,GenTrade(order,u.order,-u.order.price,isMeBuy))
				u.order.Volume -= order.Volume
				order.Volume = 0
				iterator.Close()
			}
		}
		if(value.head == nil){
			rbt.Delete(key)
		}
    })
	iterator.LessOrEqual(bound)
	if(order.Volume > 0){
		blrunner.MyTree(order).InsertOrder(order)
		if(isBuy){
			BuyVolume += order.Volume
		}else{
			SellVolume += order.Volume
		}
	}
	return trades
}

func (blrunner *BLRunner) oppoBest(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.OtherTree(order)
	isMeBuy := rbt == blrunner.SellTree
	trades := make([]common.BLTrade,0,1)
	if(isMeBuy && SellVolume == 0 || !isMeBuy && BuyVolume == 0) return trades
	price,_ = rbt.Min()
	order.Price = -price
	return blrunner.dealLimit(order)
}

func (blrunner *BLRunner) selfBest(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.MyTree(order)
	isMeBuy := rbt == blrunner.BuyTree
	trades := make([]common.BLTrade,0,1)
	if(isMeBuy && BuyVolume == 0 || !isMeBuy && SellVolume == 0) return trades
	price,_ = rbt.Max()
	order.Price = price
	return blrunner.dealLimit(order)
}

func (blrunner *BLRunner) insOnce(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.OtherTree(order)
	isMeBuy := rbt == blrunner.SellTree
	trades := make([]common.BLTrade,0,1)
	iterator, err := rbt.NewRbIterator(func(iterator RbIterator, key RbKey, value interface{}){
        u := value.head
		for ;u != nil && !iterator.Closed();u = value.head{
			if(u.order.Volume <= order.Volume){
				trades = append(trades,GenTrade(order,u.order,-u.order.price,isMeBuy))
				order.Volume -= u.order.Volume
				value.Pop(blrunner.QueueSlab)
			}else{
				trades = append(trades,GenTrade(order,u.order,-u.order.price,isMeBuy))
				u.order.Volume -= order.Volume
				order.Volume = 0
				iterator.Close()
			}
		}
		if(value.head == nil){
			rbt.Delete(key)
		}
    })
	iterator.All()
	return trades
}

func (blrunner *BLRunner) ins5Once(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.OtherTree(order)
	isMeBuy := rbt == blrunner.SellTree
	trades := make([]common.BLTrade,0,1)
	k := 0
	iterator, err := rbt.NewRbIterator(func(iterator RbIterator, key RbKey, value interface{}){
        u := value.head
		for ;u != nil && !iterator.Closed() && k < 5;u = value.head{
			if(u.order.Volume <= order.Volume){
				trades = append(trades,GenTrade(order,u.order,-u.order.price,isMeBuy))
				order.Volume -= u.order.Volume
				value.Pop(blrunner.QueueSlab)
			}else{
				trades = append(trades,GenTrade(order,u.order,-u.order.price,isMeBuy))
				u.order.Volume -= order.Volume
				order.Volume = 0
				iterator.Close()
			}
			k++
		}
		if(value.head == nil){
			rbt.Delete(key)
		}
    })
	iterator.All()
	return trades
}

func (blrunner *BLRunner) allinOnce(order *common.BLOrder) []common.BLTrade {
	if order.Direction == common.DirBuy {
		if order.Volume <= blrunner.SellVolume {
			return blrunner.insOnce(order)
			} else {
				return []common.BLTrade{}
			}
			} else {
				if order.Volume <= blrunner.BuyTree {
					return blrunner.insOnce(order)
					} else {
						return []common.BLTrade{}
					}
	}
}


func (rbt *arbt.RbTree) InsertOrder(order *common.BLOrder){
	price := Float64Key(order.Price)
	if node,ok := rbt.Get(&price) ; ok{
		node.Push(blrunner.Chunk,order)
	}else{
		node = new(Queue)
		node.Push(blrunner.Chunk,order)
		rbt.Insert(&price,node)
	}
}

func (blrunner *BLRunner) Load(lower float64,upper float64) {
	blrunner.BuyTree = NewRbTree()
	blrunner.SellTree = NewRbTree()
	blrunner.LowerPrice = lower
	blrunner.UpperPrice = upper
	const ChunkSize = 1048576 
	QueueSlab = NewChunk(ChunkSize)
	buyFile,errB := os.OpenFile("./buy_cache",os.RDONLY,0777)
	sellFile,errS := os.OpenFile("./sell_cache",os.RDONLY,0777)
	if(errB != nil || errS != nil){
		blrunner.BuyVolume = blrunner.SellVolume = 0
		if(errB == nil){
			buyFile.Close()
		}
		if(errS == nil){
			sellFile.Close()
		}
		return
	}
	buf := make([]byte,8)
	if n,err := buyFile.Read(buf);err == nil{
		rbuf := bytes.NewReader(buf)
		binary.Read(rbuf,binary.LittleEndian,&blrunner.BuyVolume)	
		//blrunner.BuyVolume = int64(buf)
	}else{
		panic("Cache corrupt")
	}
	if n,err := sellFile.Read(buf);err == nil{
		rbuf := bytes.NewReader(buf)
		binary.Read(rbuf,binary.LittleEndian,&blrunner.SellVolume)	
		//blrunner.SellVolume = int64(buf)
	}else{
		panic("Cache corrupt")
	}
	parse := func(data []byte) *common.BLOrder {
		rbuf := bytes.NewReader(data)
		order := new(common.BLOrder)
		binary.Read(rbuf,binary.LittleEndian,&order.StkCode)
		binary.Read(rbuf,binary.LittleEndian,&order.OrderId)
		binary.Read(rbuf,binary.LittleEndian,&order.Direction)
		binary.Read(rbuf,binary.LittleEndian,&order.Type)
		binary.Read(rbuf,binary.LittleEndian,&order.Price)
		binary.Read(rbuf,binary.LittleEndian,&order.Volume)
		return order
	}
	buf = make([]byte,28)
	for{
		if n,err := buyFile.Read(buf);err == nil{
			blrunner.BuyTree.InsertOrder(parse(buf))
		}else{
			break
		}
	}
	for{
		if n,err := sellFile.Read(buf);err == nil{
			blrunner.SellTree.InsertOrder(parse(buf))
		}else{
			break
		}
	}
	buyFile.Close()
	sellFile.Close()
}



func (blrunner *BLRunner) Dump() {
	buyFile,errB := os.OpenFile("./buy_cache",os.WRONLY,0777)
	sellFile,errS := os.OpenFile("./sell_cache",os.WRONLY,0777)
	if(errB != nil || errS != nil){
		panic("Failed to write cache")
	}
	wbuf := new(bytes.Buffer)
	_ := binary.Write(wbuf,binary.LittleEndian,BuyVolume)
	_,_ := buyFile.Write(wbuf.Bytes)
	wbuf = new(bytes.Buffer)
	_ := binary.Write(wbuf,binary.LittleEndian,SellVolume)
	_,_ := sellFile.Write(wbuf.Bytes)
	iterator, err := blrunner.BuyTree.NewRbIterator(func(iterator RbIterator, key RbKey, value interface{}){
        u := value.head
		for ;u != nil;u = u.next {
			wbuf := new(bytes.Buffer)
			_ := binary.Write(wbuf,binary.LittleEndian,StkCode)
			_ := binary.Write(wbuf,binary.LittleEndian,OrderId)
			_ := binary.Write(wbuf,binary.LittleEndian,Direction)
			_ := binary.Write(wbuf,binary.LittleEndian,Type)
			_ := binary.Write(wbuf,binary.LittleEndian,Price)
			_ := binary.Write(wbuf,binary.LittleEndian,Volume)
			_,_ := buyFile.Write(wbuf.Bytes)
		}
    })
	iterator.All()
	iterator, err := blrunner.SellTree.NewRbIterator(func(iterator RbIterator, key RbKey, value interface{}){
        u := value.head
		for ;u != nil;u = u.next {
			wbuf := new(bytes.Buffer)
			_ := binary.Write(wbuf,binary.LittleEndian,StkCode)
			_ := binary.Write(wbuf,binary.LittleEndian,OrderId)
			_ := binary.Write(wbuf,binary.LittleEndian,Direction)
			_ := binary.Write(wbuf,binary.LittleEndian,Type)
			_ := binary.Write(wbuf,binary.LittleEndian,Price)
			_ := binary.Write(wbuf,binary.LittleEndian,Volume)
			_,_ := sellFile.Write(wbuf.Bytes)
		}
    })
	iterator.All()
	buyFile.Close()
	sellFile.Close()
}
