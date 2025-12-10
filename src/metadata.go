package main

import (
	"fmt"
	"math"
	utils "mst/sublinear/utils"
	"slices"
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

func (md *GraphMetaData) MachinesPerLevel() int {
	return int(math.Ceil(float64(4*md.edges)) / md.S())
}

func logbx(base, x float64) float64 {
	return math.Log(x) / math.Log(base)
}

func (md *GraphMetaData) NumLevels() int {
	return int(math.Ceil(logbx(md.S()/4, float64(md.edges))))
}

func CreateMultiTree(edges []*utils.Edge, md *GraphMetaData) ([]*NodeData, error) {
	nodeGenerator := NewNodeDataGenerator()

	levels := md.NumLevels()
	machinesPerLevel := md.MachinesPerLevel()
	baseEdgesList, err := utils.Partition(edges, machinesPerLevel)
	if err != nil {
		return nil, fmt.Errorf("failed to partition edges: %v", err)
	}

	nodes := []*NodeData{}
	// leaf nodes
	for _, nodeEdges := range baseEdgesList {
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
