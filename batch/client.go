package batch

import (
	"net/rpc"
	"path/filepath"
)

type Client struct {
	*rpc.Client
}

func NewClient(path string) (*Client, error) {
	cl, err := rpc.Dial("unix", path)
	return &Client{cl}, err
}

func (cl *Client) Run(task Task) (Result, error) {
	if path, err := filepath.Abs(task.Executable); err == nil {
		task.Executable = path
	}

	result := Result{}
	err := cl.Call("Server.Run", task, &result)
	return result, err
}
