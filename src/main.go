package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
)

func listenOnRandomAddr() (lis net.Listener, err error) {
	log.Println("attempting to listen on random port")
	for {
		port := rand.Intn(65535-1024) + 1024
		addr := fmt.Sprintf(":%d", port)

		lis, err = net.Listen("tcp", addr)
		if err == nil {
			break
		}

		log.Printf("failed to listen on addr %s: %v", addr, err)
	}
	log.Printf("listening on port %s", lis.Addr().String())

	return lis, nil
}

func run(graphFile string, outFile string, epsilon float64) error {
	// edges, err := utils.ReadGraph(graphFile)
	// if err != nil {
	// 	return err
	// }

	return nil
}

func main() {
	if len(os.Args) != 4 {
		fmt.Println("usage: go run *.go <infile> <outfile> <epsilon>")
		os.Exit(1)
	}

	epsilon, err := strconv.ParseFloat(os.Args[3], 64)
	if err != nil {
		log.Fatalf("error parsing epsilon: %v", err)
	}
	run(os.Args[1], os.Args[2], epsilon)
}
