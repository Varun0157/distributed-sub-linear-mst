package main

import (
	"fmt"
	"log"
	"os"
	"sync"

	utils "mst/sublinear/utils"
)

func createTree(edges []*utils.Edge) ([]*NodeData, error) {
	nodeGenerator := NewNodeDataGenerator()

	nodes := []*NodeData{}
	// leaf nodes
	for _, edge := range edges {
		node, err := nodeGenerator.CreateNode()
		if err != nil {
			return nil, fmt.Errorf("failed to create node: %v", err)
		}

		node.AddEdges([]*utils.Edge{edge})
		for _, vertex := range []int32{edge.Src, edge.Dest} {
			node.UpdateFragment(vertex, vertex)
		}

		nodes = append(nodes, node)
	}

	// kind of a reverse level order traversal to build a tree from leaves

	NUM_CHILDREN := 2
	queue := make([]*NodeData, len(nodes))
	copy(queue, nodes)

	for len(queue) > 1 {
		numNodes := len(queue)
		NUM_PARENTS := numNodes / NUM_CHILDREN

		for start := 0; start < NUM_PARENTS; start++ {
			children := queue[:NUM_CHILDREN]
			queue = queue[NUM_CHILDREN:]

			parent, err := nodeGenerator.CreateNode()
			if err != nil {
				return nil, fmt.Errorf("failed to create parent node: %v", err)
			}

			childrenData := []*NodeMetaData{}
			for _, child := range children {
				childrenData = append(childrenData, child.md)
			}

			parent.md.SetChildren(childrenData)
			for _, child := range children {
				child.md.SetParent(parent.md)
			}
			// to continue the upward level order traversal
			queue = append(queue, parent)

			// add the node to the list
			nodes = append(nodes, parent)
		}
	}

	return nodes, nil
}

func calcMST(graphFile string, outFile string) error {
	log.Printf("graph file: %s", graphFile)
	log.Printf("out   file: %s", outFile)

	edges, err := utils.ReadGraph(graphFile)
	if err != nil {
		return err
	}

	nodes, err := createTree(edges)
	if err != nil {
		return fmt.Errorf("failed to create tree: %v", err)
	}

	serverWg := sync.WaitGroup{}

	for _, node := range nodes {
		// bind the server to a port
		log.Printf("node: %s", node.String())
		server, err := NewSubLinearServer(node, outFile)
		if err != nil {
			log.Fatalf("failed to create server: %v", err)
		}

		// launch the server
		serverWg.Add(1)
		go func() {
			defer serverWg.Done()

			err := func() error {
				if server.nodeData.md.isLeaf() {
					return server.leafDriver()
				} else {
					return server.nonLeafDriver()
				}
			}()
			if err != nil {
				log.Fatalf("failed to run server: %v", err)
			}

			server.ShutDown()
		}()
	}

	serverWg.Wait()

	return nil
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("usage: go run *.go <infile> <outfile>")
		os.Exit(1)
	}

	infile := os.Args[1]
	outfile := os.Args[2]

	err := calcMST(infile, outfile)
	if err != nil {
		log.Fatalf("[ERROR] failed to run: %v", err)
	}

	edges, err := utils.ReadGraph(outfile)
	if err != nil {
		log.Fatalf("[ERROR] failed to read output graph: %v", err)
	}
	weight := utils.GetWeight(edges)
	log.Printf("[INFO] weight of the graph: %d", weight)
}
