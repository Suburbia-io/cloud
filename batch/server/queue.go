package server

import (
	"time"
)

type queue struct {
	q chan task
}

func newQueue() *queue {
	return &queue{
		q: make(chan task, 100000),
	}
}

func (q *queue) Length() int {
	return len(q.q)
}

func (q *queue) Put(task task) {
	q.q <- task
}

func (q *queue) Next(timeout time.Duration) (task, bool) {
	select {
	case task := <-q.q:
		return task, true
	case <-time.After(timeout):
		return task{}, false
	}
}
