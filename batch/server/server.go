package server

import (
	"github.com/Suburbia-io/cloud/batch"
)

type Server struct {
	queue iQueue
}

func NewServer(queue iQueue) *Server {
	return &Server{
		queue: queue,
	}
}

func (srv *Server) Run(t batch.Task, respPtr *batch.Result) error {
	task := task{
		Executable: t.Executable,
		Env:        t.Env,
		Args:       t.Args,
		Result:     make(chan batch.Result, 1),
	}
	srv.queue.Put(task)
	*respPtr = <-task.Result
	return nil
}
