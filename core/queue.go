package core

import (
	"sync"
)

type LinkNode struct {
	next    *LinkNode
	OrderId int32
	Volume  int32
}

type Queue struct {
	head *LinkNode
	tail *LinkNode
}

func (queue *Queue) Push(p *sync.Pool, oid int32, vol int32) {
	v := p.Get().(*LinkNode)
	v.OrderId = oid
	v.Volume = vol
	v.next = nil
	if queue.head == nil {
		queue.head, queue.tail = v, v
	} else {
		queue.tail.next = v
		queue.tail = v
	}
}

func (queue *Queue) Free(p *sync.Pool) {
	if queue.head == nil {
		return
	}
	if queue.head == queue.tail {
		p.Put(queue.head)
		queue.head, queue.tail = nil, nil
		return
	}
	hn := queue.head.next
	p.Put(queue.head)
	queue.head = hn
}
