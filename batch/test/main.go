package main

import (
	"log"

	"github.com/Suburbia-io/cloud/batch"
)

type MyArgs struct {
	X int
	Y int
}

func MyFunc(args *MyArgs) error {
	log.Printf("X*Y=%d", args.X*args.Y)
	return nil
}

func main() {
	batch.Register(MyFunc)
	batch.Init()
	cl, err := batch.NewClient("../cmd/batchd/socket")
	if err != nil {
		panic(err)
	}

	env := map[string]string{}

	resp, err := cl.RunFunc(env, MyFunc, MyArgs{
		X: 10,
		Y: 15,
	})
	if err != nil {
		panic(err)
	}
	log.Printf("Code: %d", resp.Code)
	log.Printf("Output: %s", resp.Output)
}
