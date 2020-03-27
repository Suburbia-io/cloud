package server

import (
	"flag"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
)

func Main() {
	var (
		apiKey     string
		apiSecret  string
		socketPath string
		sshKeyPath string
	)

	flag.StringVar(&apiKey, "api-key", "", "REQUIRED: Exoscale API key.")
	flag.StringVar(&apiSecret, "api-secret", "", "REQUIRED: Exoscale API secret.")
	flag.StringVar(&socketPath, "socket", "", "REQUIRED: Path to socket.")
	flag.StringVar(&sshKeyPath, "ssh-key", "", "Path to private ssh key to use.")
	flag.Parse()

	if apiKey == "" || apiSecret == "" || socketPath == "" {
		flag.Usage()
		os.Exit(1)
	}

	if sshKeyPath == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			panic(err)
		}
		sshKeyPath = filepath.Join(homeDir, ".ssh", "id_rsa")
	}

	queue := newQueue()
	runVMSupervisor(queue, apiKey, apiSecret, sshKeyPath)
	srv := NewServer(queue)

	if err := rpc.Register(srv); err != nil {
		log.Fatalf("Failed to register server for RPC: %v", err)
	}

	os.Remove(socketPath)

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("Failed to listen on socket %s: %v", socketPath, err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}
