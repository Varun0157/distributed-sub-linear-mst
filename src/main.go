package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	utils "mst/sublinear/utils"
)

func createTree(edges []*utils.Edge) ([]*NodeData, error) {
	nodeGenerator := NewNodeDataGenerator()

	nodes := []*NodeData{}
	for _, edge := range edges {
		node, err := nodeGenerator.CreateNode()
		if err != nil {
			return nil, fmt.Errorf("failed to create node: %v", err)
		}

		node.AddEdges([]*utils.Edge{edge})
		for _, vertex := range []int{int(edge.Src), int(edge.Dest)} {
			node.UpdateFragment(vertex, vertex)
		}
		node.SetType(LEAF)

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

			parent.SetType(INTERMEDIATE)
			parent.SetChildren(children)
			for _, child := range children {
				child.SetParent(parent)
			}
			// to continue the upward level order traversal
			queue = append(queue, parent)

			// add the node to the list
			nodes = append(nodes, parent)
		}
	}

	if len(queue) > 0 {
		root := queue[0]
		root.SetType(ROOT)
	}

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

	servers := []*SubLinearServer{}
	for _, node := range nodes {
		log.Printf("node: %s", node.String())
		server, err := NewSubLinearServer(node)
		if err != nil {
			return fmt.Errorf("failed to create server: %v", err)
		}

		if len(server.nodeData.children) > 0 {
			server.nodeData.childReqWg.Add(len(node.children))
			go server.upwardPropListener()
		}

		servers = append(servers, server)
	}

	serverWg := sync.WaitGroup{}
	phase := 1
	for phase < 10 {
		log.Printf("PHASE %d\n", phase)

		for _, s := range servers {
			if s.nodeData.nodeType != LEAF {
				continue
			}
			serverWg.Add(1)
			go func() {
				defer serverWg.Done()

				update, err := s.sendEdgesUp()
				if err != nil {
					log.Fatalf("failed to send edges up: %v", err)
				}

				for srcFrag, trgFrag := range update.GetUpdates() {
					for node, frag := range s.nodeData.fragments {
						if frag != int(srcFrag) {
							continue
						}
						s.nodeData.UpdateFragment(node, int(trgFrag))
					}
				}
			}()
		}
		serverWg.Wait()

		phase++
		time.Sleep(2 * time.Second)
	}

	for _, s := range servers {
		if s.nodeData.nodeType != LEAF {
			continue
		}
		log.Printf("node: %s", s.nodeData.String())
	}

	return nil
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("usage: go run *.go <infile> <outfile>")
		os.Exit(1)
	}

	infile := os.Args[1]
	outfile := os.Args[2]

	err := run(infile, outfile)
	if err != nil {
		log.Fatalf("[ERROR] failed to run: %v", err)
	}
}
