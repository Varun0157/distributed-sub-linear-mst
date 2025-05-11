package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"slices"
	"strconv"
	"sync"

	utils "mst/sublinear/utils"
)

type GraphMetaData struct {
	vertices int32
	edges    int32
	alpha    float64
}

func NewMetaData(edges []*utils.Edge, alpha float64) *GraphMetaData {
	numVertices, numEdges, _ := utils.GetStats(edges)
	return &GraphMetaData{
		vertices: int32(numVertices),
		edges:    int32(numEdges),
		alpha:    alpha,
	}
}

func (md *GraphMetaData) S() float64 {
	return math.Pow(float64(md.vertices), md.alpha)
}

func (md *GraphMetaData) NumEdgesPerNode() int32 {
	return int32(math.Floor(md.S()))
}

func createTree(edges []*utils.Edge, md *GraphMetaData) ([]*NodeData, error) {
	nodeGenerator := NewNodeDataGenerator()

	numNodes := int(math.Ceil(float64(len(edges)) / float64(md.NumEdgesPerNode())))
	nodeEdgesList, err := utils.Partition(edges, numNodes)
	if err != nil {
		return nil, fmt.Errorf("failed to partition edges: %v", err)
	}

	nodes := []*NodeData{}
	// leaf nodes
	for _, nodeEdges := range nodeEdgesList {
		node, err := nodeGenerator.CreateNode()
		if err != nil {
			return nil, fmt.Errorf("failed to create node: %v", err)
		}

		node.AddEdges(nodeEdges)
		for _, edge := range nodeEdges {
			for _, vertex := range []int32{edge.U, edge.V} {
				node.UpdateFragment(vertex, vertex)
			}
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

	// NOTE: we start-up the servers in ROOT to LEAF order to ensure
	// the servers are ready to receive messages
	slices.Reverse(nodes)
	return nodes, nil
}

func calcMST(graphFile string, outFile string, alpha float64) error {
	log.Printf("graph file: %s", graphFile)
	log.Printf("out   file: %s", outFile)

	edges, err := utils.ReadGraph(graphFile)
	if err != nil {
		return err
	}
	md := NewMetaData(edges, alpha)

	nodes, err := createTree(edges, md)
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

	var maxPhase int32 = 0
	for _, node := range nodes {
		maxPhase = max(maxPhase, node.md.phase)
	}
	log.Printf("===> calculation complete in %d rounds", maxPhase)

	return nil
}

func stats(infile, outfile string) {
	graph, err := utils.ReadGraph(infile)
	if err != nil {
		log.Fatalf("[ERROR] failed to read input graph: %v", err)
	}
	v, e, w := utils.GetStats(graph)
	log.Printf("[INFO] graph ->  %d verts, %d edges ,%d weight", v, e, w)

	mst, err := utils.ReadGraph(outfile)
	if err != nil {
		log.Fatalf("[ERROR] failed to read output graph: %v", err)
	}
	v, e, w = utils.GetStats(mst)
	log.Printf("[INFO] mst   ->  %d verts, %d edges ,%d weight", v, e, w)
}

func main() {
	if len(os.Args) != 4 {
		fmt.Println("usage: go run *.go <infile> <outfile> <alpha>")
		os.Exit(1)
	}

	infile := os.Args[1]
	outfile := os.Args[2]
	alpha, err := strconv.ParseFloat(os.Args[3], 64)
	if err != nil {
		log.Fatalf("[ERROR] failed to parse alpha: %v", err)
	}

	err = calcMST(infile, outfile, alpha)
	if err != nil {
		log.Fatalf("[ERROR] failed to run: %v", err)
	}

	stats(infile, outfile)
}
