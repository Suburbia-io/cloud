package batch

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Client struct {
	*rpc.Client
}

func NewClient(path string) (*Client, error) {
	cl, err := rpc.Dial("unix", path)
	return &Client{Client: cl}, err
}

func (cl *Client) Run(task Task) (Result, error) {
	if path, err := filepath.Abs(task.Executable); err == nil {
		task.Executable = path
	}

	result := Result{}
	err := cl.Call("Server.Run", task, &result)
	return result, err
}

// Run runs the function on the given arguments. The function must be
// registered with the `Register` function. See also: `Init` function.
func (cl *Client) RunFunc(
	env map[string]string,
	fn interface{},
	args interface{},
) (
	Result,
	error,
) {

	// Encode arguments.
	buf := &bytes.Buffer{}
	if err := gob.NewEncoder(buf).Encode(args); err != nil {
		return Result{
			Code:   -1,
			Output: fmt.Sprintf("Failed to encode arguments: %v", err),
		}, err
	}

	// Create args strings.
	b := strings.Builder{}
	b.WriteString("__BATCH__ ")
	b.WriteString(functionName(fn))
	b.WriteString(" ")
	b.WriteString(hex.EncodeToString(buf.Bytes()))

	return cl.Run(Task{
		Executable: os.Args[0],
		Env:        env,
		Args:       b.String(),
	})
}

// Run runs the function on the given list of arguments. See RunFunc.
func (cl *Client) MapFunc(
	env map[string]string,
	fn interface{},
	argsList ...interface{},
) []Result {
	results := make([]Result, len(argsList))

	wg := sync.WaitGroup{}
	wg.Add(len(argsList))

	fmt.Fprint(os.Stderr, "\n")
	for i, args := range argsList {
		go func(i int, args interface{}) {
			result, err := cl.RunFunc(env, fn, args)
			if err != nil {
				result = Result{
					Code:   -1,
					Output: fmt.Sprintf("Failed to run function: %v", err),
				}
			}
			results[i] = result

			if result.Code == 0 {
				fmt.Fprint(os.Stderr, ".")
			} else {
				fmt.Fprint(os.Stderr, "X")
			}

			wg.Done()
		}(i, args)
	}
	wg.Wait()
	fmt.Fprint(os.Stderr, "\n")

	return results
}
