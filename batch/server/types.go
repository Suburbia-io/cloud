package server

import (
	"time"

	"github.com/Suburbia-io/cloud/batch"
)

type stateFunc func() stateFunc

type task struct {
	Executable string
	Env        map[string]string
	Args       string
	Result     chan batch.Result
}

type iVMSupervisor interface {
	DeleteVM(vmID string) error
}

type iQueue interface {
	Length() int
	Put(task)
	Next(timeout time.Duration) (task, bool)
}
