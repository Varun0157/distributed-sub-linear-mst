package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"

	utils "mst/sublinear/utils"
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

func run(graphFile string, outFile string) error {
	log.Printf("graph file: %s", graphFile)
	log.Printf("out file: %s", outFile)

	edges, err := utils.ReadGraph(graphFile)
	if err != nil {
		return err
	}

	nodeGenerator := NewNodeGenerator()
	nodes := []*Node{}

	for _, edge := range edges {
		node := nodeGenerator.CreateNode()
		node.SetEdges([]utils.Edge{edge})
		node.SetType(LEAF)

		nodes = append(nodes, node)
	}

	for _, node := range nodes {
		log.Printf("node: %v", node)
	}

	return nil
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("usage: go run *.go <infile> <outfile>")
		os.Exit(1)
	}

	run(os.Args[1], os.Args[2])
}
