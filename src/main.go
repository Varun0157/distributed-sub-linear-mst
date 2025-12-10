package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	utils "mst/sublinear/utils"
)

func calcMST(graphFile string, outFile string, alpha float64) error {
	log.Printf("graph file: %s", graphFile)
	log.Printf("out   file: %s", outFile)

	edges, err := utils.ReadGraph(graphFile)
	if err != nil {
		return err
	}
	md := NewMetaData(edges, alpha)

	nodes, err := CreateMultiTree(edges, md)
	if err != nil {
		return fmt.Errorf("failed to create tree: %v", err)
	}
	log.Printf("created tree with %d nodes", len(nodes))

	// serverWg := sync.WaitGroup{}
	// for _, node := range nodes {
	// 	// bind the server to a port
	// 	log.Printf("node: %s", node.String())
	// 	server, err := NewSubLinearServer(node, outFile)
	// 	if err != nil {
	// 		log.Fatalf("failed to create server: %v", err)
	// 	}
	//
	// 	// launch the server
	// 	serverWg.Add(1)
	// 	go func() {
	// 		defer serverWg.Done()
	//
	// 		err := func() error {
	// 			if server.nodeData.md.isLeaf() {
	// 				return server.leafDriver()
	// 			} else {
	// 				return server.nonLeafDriver()
	// 			}
	// 		}()
	// 		if err != nil {
	// 			log.Fatalf("failed to run server: %v", err)
	// 		}
	//
	// 		server.ShutDown()
	// 	}()
	// }
	// serverWg.Wait()
	//
	// var maxPhase int32 = 0
	// for _, node := range nodes {
	// 	maxPhase = max(maxPhase, node.md.phase)
	// }
	// log.Printf("===> calculation complete in %d rounds", maxPhase)
	//
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
