package main

import (
	"log"

	"github.com/Suburbia-io/cloud/batch"
)

func main() {
	cl, err := batch.NewClient("../cmd/batchd/socket")
	if err != nil {
		panic(err)
	}

	task := batch.Task{
		Executable: "/bin/echo",
		Env: map[string]string{
			"MY_NEW_VAR": "a-new-var",
		},
		Args: "$MY_NEW_VAR",
	}

	resp, err := cl.Run(task)
	if err != nil {
		panic(err)
	}
	log.Printf("Output: %v", resp)
}
