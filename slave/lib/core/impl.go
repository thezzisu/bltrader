package core

import (
	"encoding/binary"
	"encoding/gob"
	"github.com/thezzisu/bltrader/common"
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"github.com/emirpasic/gods/maps/treemap"

)

type ShortOrder struct{
	OrderId		int32
	Price		float64
	Volume		int32
}
func (order *ShortOrder) FromJSON(msg []byte) error {
	_ = json.Unmarshal(msg, order)
}
func (order *ShortOrder) ToJSON() []byte {
	str, _ := json.Marshal(order)
	return str
}

type BLRunner struct {
	buyTree *treemap
	sellTree *treemap
	buyVolume int32
	sellVolume int32
	lowerPrice float64
	upperPrice float64
	queuePool *sync.Pool
}

func (blrunner *BLRunner) Dispatch(order *common.BLOrder) []common.BLTrade {
	if order.Price > blrunner.UpperPrice || order.Price < blrunner.LowerPrice {
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

func (blrunner *BLRunner) MyTree(order *common.BLOrder) *treemap {
	if(order.Type == common.DirBuy){
		return blrunner.buyTree
	}else{
		return blrunner.sellTree
	}
}
func (blrunner *BLRunner) OtherTree(order *common.BLOrder) *treemap {
	if(order.Type == common.DirBuy){
		return blrunner.buyTree
	}else{
		return blrunner.sellTree
	}
}

func (blrunner *BLRunner) GenTrade(order *common.BLOrder,ordee *ShortOrder,price float64,isMeBuy bool) common.BLTrade {
	vol := order.Volume - ordee.Volume
	if(vol < 0){
		vol = -vol
	}
	if isMeBuy{
		blrunner.sellVolume -= vol
		return common.BLTrade{StkCode:order.StkCode,BidId:order.OrderId,AskId:ordee.OrderId,Price:price,Volume:vol}
	}else{
		blrunner.buyVolume -= vol
		return common.BLTrade{StkCode:order.StkCode,BidId:ordee.OrderId,AskId:order.OrderId,Price:price,Volume:vol}
	}
}

func (blrunner *BLRunner) dealLimit(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.OtherTree(order)
	isMeBuy := order.Direction == DirBuy
	trades := make([]common.BLTrade,0,1)
	shorter := ShortOrder{order.OrderId,order.Price,order.Volume}
	bound := order.Price
	it := rbt.Iterator()
	for it.Next() {
		oprice,q := it.Key(),it.Value()
		if(isMeBuy){
			if(oprice > bound){
				break
			}
		}else{
			if(oprice < bound){
				break
			}
		}
		u := q.head
		endFlag := false
		for ;u != nil && !endFlag;u = q.head{
			if(u.order.Volume <= shorter.Volume){
				trades = append(trades,GenTrades(order,u.order,u.order.price,isMeBuy))
				order.Volume -= u.order.Volume
				q.Free(blrunner.queuePool)
			}else{
				trades = append(trades,GenTrades(order,u.order,u.order.price,isMeBuy))
				u.order.Volume -= shorter.Volume
				shorter.Volume = 0
				endFlag = true
			}
		}
		if(q.head == nil){
			rbt.Remove(oprice)
		}
	}
	if(shorter.Volume > 0){
		blrunner.InsertOrder(blrunner.MyTree(order),shorter)
		if(isBuy){
			blrunner.buyVolume += shorter.Volume
		}else{
			blrunner.sellVolume += shorter.Volume
		}
	}
	return trades
}

func (blrunner *BLRunner) oppoBest(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.OtherTree(order)
	isMeBuy := order.Direction == DirBuy
	trades := make([]common.BLTrade,0,1)
	if(isMeBuy && blrunner.sellVolume == 0 || !isMeBuy && blrunner.buyVolume == 0){
		return trades
	}
	shorter := ShortOrder{order.OrderId,order.Price,order.Volume}
	shorter.Price,q := rbt.Min()
	u := q.head
	endFlag := false
	for ;u != nil && !endFlag;u = q.head{
		if(u.order.Volume <= shorter.Volume){
			trades = append(trades,GenTrades(order,u.order,u.order.price,isMeBuy))
			shorter.Volume -= u.order.Volume
			q.Free(blrunner.queuePool)
		}else{
			trades = append(trades,GenTrades(order,u.order,u.order.price,isMeBuy))
			u.order.Volume -= shorter.Volume
			shorter.Volume = 0
			endFlag = true
		}
	}
	if(q.head == nil){
		rbt.Remove(shorter.Price)
	}
	if(shorter.Volume > 0){
		blrunner.InsertOrder(blrunner.MyTree(order),shorter)
		if(isBuy){
			blrunner.buyVolume += shorter.Volume
		}else{
			blrunner.sellVolume += shorter.Volume
		}
	}
	return trades
}

func (blrunner *BLRunner) selfBest(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.OtherTree(order)
	isMeBuy := order.Direction == DirBuy
	trades := make([]common.BLTrade,0,1)
	if(isMeBuy && blrunner.buyVolume == 0 || !isMeBuy && blrunner.sellVolume == 0){
		return trades
	}
	shorter := ShortOrder{order.OrderId,order.Price,order.Volume}
	shorter.Price,q := rbt.Max()
	q.Push(blrunner.queuePool,shorter)
	return trades
}

func (blrunner *BLRunner) insOnce(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.OtherTree(order)
	isMeBuy := order.Direction == DirBuy
	trades := make([]common.BLTrade,0,1)
	shorter := ShortOrder{order.OrderId,order.Price,order.Volume}
	it := rbt.Iterator()
	for it.Next() {
		oprice,q := it.Key(),it.Value()
		u := q.head
		endFlag := false
		for ;u != nil && !endFlag;u = q.head{
			if(u.order.Volume <= shorter.Volume){
				trades = append(trades,GenTrades(order,u.order,u.order.price,isMeBuy))
				shorter.Volume -= u.order.Volume
				q.Free(blrunner.queuePool)
			}else{
				trades = append(trades,GenTrades(order,u.order,u.order.price,isMeBuy))
				u.order.Volume -= shorter.Volume
				shorter.Volume = 0
				endFlag = true
			}
		}
		if(q.head == nil){
			rbt.Remove(oprice)
		}
	}
	return trades
}

func (blrunner *BLRunner) ins5Once(order *common.BLOrder) []common.BLTrade {
	rbt := blrunner.OtherTree(order)
	isMeBuy := order.Direction == DirBuy
	trades := make([]common.BLTrade,0,1)
	shorter := ShortOrder{order.OrderId,order.Price,order.Volume}
	it := rbt.Iterator()
	k := 0
	for it.Next() {
		oprice,q := it.Key(),it.Value()
		u := q.head
		endFlag := false
		for ;u != nil && !endFlag;u = q.head{
			if(u.order.Volume <= shorter.Volume){
				trades = append(trades,GenTrades(order,u.order,u.order.price,isMeBuy))
				shorter.Volume -= u.order.Volume
				q.Free(blrunner.queuePool)
			}else{
				trades = append(trades,GenTrades(order,u.order,u.order.price,isMeBuy))
				u.order.Volume -= shorter.Volume
				shorter.Volume = 0
				endFlag = true
			}
		}
		if(q.head == nil){
			rbt.Remove(oprice)
		}
		if k++;k >= 5{
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
	c1 := a.(ShortOrder)
	c2 := b.(ShortOrder)
	switch {
	case c1.Price > c2.Price:
		return 1
	case c1.Price < c2.Price:
		return -1
	default:
		return 0
	}
}
func byPriceDescend(a, b interface{}) int {
	c1 := a.(ShortOrder)
	c2 := b.(ShortOrder)
	switch {
	case c1.Price < c2.Price:
		return 1
	case c1.Price > c2.Price:
		return -1
	default:
		return 0
	}
}

func (blrunner *BLRunner) InsertOrder(rbt *treemap,order *ShortOrder){
	if q,ok := rbt.Get(order.Price) ; !ok{
		q = new(Queue)
		rbt.Put(order.Price,q)
	}
	q.Push(blrunner.queuePool,order)
}

func (blrunner *BLRunner) Load(lower float64,upper float64){
	blrunner.buyTree = treemap.NewWith(byPriceDescend)
	blrunner.sellTree = treemap.NewWith(byPriceAscend)
	blrunner.LowerPrice = lower
	blrunner.UpperPrice = upper
	blrunner.queuePool = sync.pool{
		New: func() interface{} {
			return new(LinkNode)
		}
	}
	bFile,errB := os.OpenFile("./buy_cache",os.RDONLY,0777)
	sFile,errS := os.OpenFile("./sell_cache",os.RDONLY,0777)
	if(errB != nil || errS != nil){
		blrunner.buyVolume , blrunner.sellVolume = 0
		if(errB == nil){
			bFile.Close()
		}
		if(errS == nil){
			sFile.Close()
		}
		return
	}
	buf := make([]byte,4)
	rb := bytes.NewReader(buf)
	if _,err := bFile.Read(buf);err == nil{
		binary.Read(rb,binary.LittleEndian,&blrunner.buyVolume)
	}else{
		panic("Cache corrupt")
	}
	rb.reset()
	if _,err := sFile.Read(buf);err == nil{
		binary.Read(rb,binary.LittleEndian,&blrunner.sellVolume)
	}else{
		panic("Cache corrupt")
	}
	rb.reset()
	buf = make([]byte,28)
	for{
		if _,err := bFile.Read(buf);err == nil{
			order := new(ShortOrder)
			binary.Read(rb,binary.LittleEndian,&order)
			blrunner.InsertOrder(blrunner.buyTree,blrunner.queuePool,order)
			rb.reset()
		}else{
			break
		}
	}
	for{
		if _,err := sFile.Read(buf);err == nil{
			order := new(ShortOrder)
			binary.Read(rb,binary.LittleEndian,&order)
			blrunner.InsertOrder(blrunner.sellTree,blrunner.queuePool,order)
			rb.reset()
		}else{
			break
		}
	}
	bFile.Close()
	sFile.Close()
}

func (blrunner *BLRunner) Dump()
{
	buyFile,errB := os.OpenFile("./buy_cache",os.WRONLY,0777)
	sellFile,errS := os.OpenFile("./sell_cache",os.WRONLY,0777)
	if(errB != nil || errS != nil){
		panic("Failed to write cache")
	}
	wb := new(bytes.Buffer)
	_ = binary.Write(wb,binary.LittleEndian,blrunner.buyVolume)
	_,_ = bFile.Write(wb.Bytes())
	wb.Reset()
	_ = binary.Write(wb,binary.LittleEndian,blrunner.sellVolume)
	_,_ = bFile.Write(wb.Bytes())
	wb.Reset()
	it := blrunner.buyTree.Iterator()
	for it.Next(){
		u := it.Value().head
		for ;u != nil;u = u.next{
			_ = binary.Write(wb,binary.LittleEndian,u.order)
			_,_ := bFile.Write(wb.Bytes())
			wb.reset()
		}
	}
	it = blrunner.sellTree.Iterator()
	for it.Next(){
		u := it.Value().head
		for ;u != nil;u = u.next{
			_ = binary.Write(wb,binary.LittleEndian,u.order)
			_,_ := sFile.Write(wb.Bytes())
			wb.reset()
		}
	}
	bFile.Close()
	sFile.Close()
}