package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	utils "mst/sublinear/utils"
)

type GraphMetaData struct {
	vertices int32
	edges    int32
	alpha    float64
}

func NewMetaData(edges []*utils.Edge, alpha float64) *GraphMetaData {
	numVertices, numEdges, _ := utils.GetStats(edges)
	md := &GraphMetaData{
		vertices: int32(numVertices),
		edges:    int32(numEdges),
		alpha:    alpha,
	}

	log.Printf("GraphMetaData: vertices=%d, edges=%d, alpha=%f", md.vertices, md.edges, md.alpha)
	log.Printf("S = %f", md.S())
	log.Printf("Machines per level = %d", md.MachinesPerLevel())
	log.Printf("Num levels = %d", md.NumLevels())
	for i := range 10 {
		log.Printf("responsibleProbability(level=%d) = %v", i, md.responsibleProbability(i))
	}

	return md
}

func (md *GraphMetaData) S() float64 {
	return math.Pow(float64(md.vertices), md.alpha)
}

func (md *GraphMetaData) MachinesPerLevel() int {
	return int(math.Ceil(float64(4.0*md.edges) / md.S()))
}

func logbx(base, x float64) float64 {
	return math.Log(x) / math.Log(base)
}

func (md *GraphMetaData) NumLevels() int {
	return int(math.Ceil(logbx(md.S()/4, float64(md.edges))))
}

func (md *GraphMetaData) responsibleProbability(childLevel int) float64 {
	return math.Min(math.Pow(4.0/md.S(), float64(childLevel)), 1.0)
}

func (md *GraphMetaData) IsResponsible(childLevel int) bool {
	return rand.Float64() < md.responsibleProbability(childLevel)
}

func createNonLeafLevels(numLevels, machinesPerLevel int, nodeGenerator *NodeDataGenerator, md *GraphMetaData, fragments []int) ([][]*NodeData, error) {
	maxAssignmentAttempts := 5
	createLevel := func(i int) ([]*NodeData, error) {
		// create the required machines
		level := make([]*NodeData, 0)
		for range machinesPerLevel {
			node, err := nodeGenerator.CreateNode()
			if err != nil {
				return nil, fmt.Errorf("failed to create node: %v", err)
			}
			level = append(level, node)
		}

		// assign fragments to nodes
		assignFragments := func() error {
			for _, fragment := range fragments {
				owned := false
				for _, node := range level {
					if !md.IsResponsible(i) {
						continue
					}
					node.ownFragment(fragment)
					owned = true
				}
				if !owned {
					return fmt.Errorf("failed to assign fragment %d to any node at level %d", fragment, i)
				}
			}

			return nil
		}

		// fallback: assign all to first fragment
		assignToFirst := func() {
			firstNode := level[0]
			for _, fragment := range fragments {
				firstNode.ownFragment(fragment)
			}
		}

		assigned := false
		for range maxAssignmentAttempts {
			err := assignFragments()
			if err == nil {
				assigned = true
				break
			}

			log.Printf("assignment attempt failed at level %d: %v, trying again", i, err)
			for _, node := range level {
				node.disownAllFragments()
			}
		}

		if !assigned {
			log.Printf("unable to assign fragments at level %d, assigning all to first node", i)
			assignToFirst()
		}

		return level, nil
	}

	levels := make([][]*NodeData, 0)

	for i := 1; i < numLevels; i++ {
		level, err := createLevel(i)
		if err != nil {
			return nil, fmt.Errorf("failed to create level %d: %v", i, err)
		}
		levels = append(levels, level)
	}

	return levels, nil
}

func assignEdges(levels [][]*NodeData, fragments []int) error {
	findParent := func(level, fragment int) (*NodeData, error) {
		candidates := []*NodeData{}
		for _, node := range levels[level] {
			if node.ownsFragment(fragment) {
				candidates = append(candidates, node)
			}
		}

		if len(candidates) == 0 {
			return nil, fmt.Errorf("no owners of fragment %d in level %d", fragment, level)
		}

		randomIndex := rand.Intn(len(candidates))
		parent := candidates[randomIndex]

		return parent, nil
	}

	for i := range len(levels) - 1 {
		children := levels[i]

		for _, child := range children {
			for _, frag := range fragments {
				parent, err := findParent(i+1, frag)
				if err != nil {
					return err
				}

				child.md.SetParent(int32(frag), parent.md)
				parent.md.SetChild(int32(frag), child.md)
			}
		}
	}

	return nil
}

func CreateMultiTree(edges []*utils.Edge, md *GraphMetaData) ([][]*NodeData, error) {
	nodeGenerator := NewNodeDataGenerator()

	levels := md.NumLevels()
	if levels < 2 {
		levels = 2
	}

	machinesPerLevel := md.MachinesPerLevel()
	if machinesPerLevel > len(edges) {
		machinesPerLevel = len(edges)
	}
	baseEdgesList, err := utils.Partition(edges, machinesPerLevel)
	if err != nil {
		return nil, fmt.Errorf("failed to partition edges: %v", err)
	}

	// leaf nodes
	leafNodes := []*NodeData{}
	for _, nodeEdges := range baseEdgesList {
		node, err := nodeGenerator.CreateNode()
		if err != nil {
			return nil, fmt.Errorf("failed to create node: %v", err)
		}

		node.AddEdges(nodeEdges)
		for _, edge := range nodeEdges {
			for _, vertex := range []int32{edge.U, edge.V} {
				node.UpdateFragment(vertex, vertex)
				node.ownFragment(int(vertex))
			}
		}

		leafNodes = append(leafNodes, node)
	}

	uniqueFragments := make(map[int32]bool)
	for _, node := range leafNodes {
		for _, fragment := range node.GetFragments() {
			uniqueFragments[int32(fragment)] = true
		}
	}
	fragments := []int{}
	for frag := range uniqueFragments {
		fragments = append(fragments, int(frag))
	}

	nonLeafLevels, err := createNonLeafLevels(levels, machinesPerLevel, nodeGenerator, md, fragments)
	if err != nil {
		return nil, fmt.Errorf("failed to create non-leaf levels: %v", err)
	}

	allLevels := append([][]*NodeData{leafNodes}, nonLeafLevels...)
	if err = assignEdges(allLevels, fragments); err != nil {
		return nil, err
	}

	return allLevels, nil
}
