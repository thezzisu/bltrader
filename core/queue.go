package core

import (
	"sync"
)

type LinkNode struct {
	next  *LinkNode
	order ShortOrder
}

type Queue struct {
	head *LinkNode
	tail *LinkNode
}

func (queue *Queue) Push(p *sync.Pool, order *ShortOrder) {
	v := p.Get().(*LinkNode)
	v.order.OrderId = order.OrderId
	v.order.Price = order.Price
	v.order.Volume = order.Volume
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
