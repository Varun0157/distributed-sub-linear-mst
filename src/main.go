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

func createTree(edges []utils.Edge) ([]*Node, error) {
	nodeGenerator := NewNodeGenerator()
	nodes := []*Node{}

	for _, edge := range edges {
		node := nodeGenerator.CreateNode()
		node.AddEdges([]utils.Edge{edge})
		node.SetType(LEAF)

		nodes = append(nodes, node)
	}

	// kind of a reverse level order traversal
	queue := make([]*Node, len(nodes))
	copy(queue, nodes)

	for len(queue) > 1 {
		numNodes := len(queue)

		for start := 0; start < numNodes/2; start++ {
			children := queue[:2]
			queue = queue[2:]

			parent := nodeGenerator.CreateNode()
			parent.SetType(INTERMEDIATE)
			parent.SetChildren(children)
			for _, child := range children {
				child.SetParent(parent)
			}

			nodes = append(nodes, parent)
			queue = append(queue, parent)
		}
	}

	nodes[len(nodes)-1].SetType(ROOT)

	return nodes, nil
}

func run(graphFile string, outFile string) error {
	log.Printf("graph file: %s", graphFile)
	log.Printf("out file: %s", outFile)

	edges, err := utils.ReadGraph(graphFile)
	if err != nil {
		return err
	}

	nodes, err := createTree(edges)
	if err != nil {
		return fmt.Errorf("failed to create tree: %v", err)
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

	err := run(os.Args[1], os.Args[2])
	if err != nil {
		log.Fatalf("[ERROR] failed to run: %v", err)
	}
}
