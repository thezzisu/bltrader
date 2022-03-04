package core

import "github.com/thezzisu/bltrader/common"

type Queue struct {
	head *LinkNode
	tail *LinkNode
}

type LinkNode struct {
	prev     *LinkNode
	next     *LinkNode
	nextFree *LinkNode
	order    *common.BLOrder
}

func (queue *Queue) Push(s *Chunk, order *common.BLOrder) {
	v := s.Malloc()
	v.order = order
	v.next, v.prev = nil, nil
	if queue.head == nil {
		queue.head, queue.tail = v, v
	} else {
		queue.tail.next = v
		v.prev = queue.tail
		queue.tail = v
	}
}

func (queue *Queue) Free(s *Chunk) {
	if queue.head == nil {
		return
	}
	if queue.head == queue.tail {
		s.Free(queue.head)
		queue.head, queue.tail = nil, nil
	}
	hn := queue.head.next
	s.Free(queue.head)
	queue.head = hn
}

func (queue *Queue) Pop(s *Chunk) *common.BLOrder {
	if queue.head == nil {
		return nil
	}
	if queue.head == queue.tail {
		r := queue.head.order
		s.Free(queue.head)
		queue.head, queue.tail = nil, nil
		return r
	}
	hn := queue.head.next
	r := queue.head.order
	s.Free(queue.head)
	queue.head = hn
	return r
}

type Chunk struct {
	free *LinkNode
	pool []LinkNode
}

func NewChunk(size int) *Chunk {
	s := &Chunk{pool: make([]LinkNode, size)}
	s.free = &s.pool[0]
	prev := s.free
	for i := 1; i < len(s.pool); i++ {
		curr := &s.pool[i]
		prev.nextFree = curr
		prev = curr
	}
	return s
}

func (s *Chunk) Malloc() *LinkNode {
	o := s.free
	if o == nil {
		o = &LinkNode{}
	}
	s.free = o.nextFree
	o.nextFree = o
	return o
}

func (s *Chunk) Free(o *LinkNode) {
	if o.nextFree == o {
		o.nextFree = s.free
		s.free = o
	}
}
