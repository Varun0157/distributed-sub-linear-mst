package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	utils "mst/sublinear/utils"
)

func printTreeStructure(allLevels [][]*NodeData) {
	totalNodes := 0
	for _, level := range allLevels {
		totalNodes += len(level)
	}

	log.Printf("========================================")
	log.Printf("====== TREE STRUCTURE =======")
	log.Printf("========================================")
	log.Printf("Total levels: %d", len(allLevels))
	log.Printf("Total nodes:  %d", totalNodes)
	log.Printf("")

	for levelIdx, level := range allLevels {
		levelName := "NON-LEAF"
		if levelIdx == 0 {
			levelName = "LEAF"
		} else if levelIdx == len(allLevels)-1 {
			levelName = "ROOT"
		}

		log.Printf("--- Level %d (%s) ---", levelIdx, levelName)
		log.Printf("  Machines: %d", len(level))

		for nodeIdx, node := range level {
			log.Printf("  Node-%d: %v", nodeIdx, node)
		}
		log.Printf("")
	}

	log.Printf("========================================")
}

func calcMST(graphFile string, outFile string, alpha float64) error {
	log.Printf("graph file: %s", graphFile)
	log.Printf("out   file: %s", outFile)

	edges, err := utils.ReadGraph(graphFile)
	if err != nil {
		return err
	}
	md := NewMetaData(edges, alpha)

	levels, err := CreateMultiTree(edges, md)
	if err != nil {
		return fmt.Errorf("failed to create tree: %v", err)
	}

	// Print tree structure
	printTreeStructure(levels)

	// Flatten levels into nodes array for execution
	nodes := []*NodeData{}
	for _, level := range levels {
		nodes = append(nodes, level...)
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
		go func(s *SubLinearServer) {
			defer serverWg.Done()

			err := func() error {
				if s.nodeData.md.isLeaf() {
					return s.leafDriver()
				} else {
					return s.nonLeafDriver()
				}
			}()
			if err != nil {
				log.Fatalf("failed to run server: %v", err)
			}

			s.ShutDown()
		}(server)
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
