package main

import (
	"fmt"
	"log"
	"sync"

	utils "mst/sublinear/utils"
)

type NodeType int

const (
	ROOT NodeType = iota
	LEAF
	INTERMEDIATE
	UNKNOWN
)

type NodeData struct {
	id       uint64
	nodeType NodeType

	edges     []utils.Edge
	parent    *NodeData
	children  []*NodeData
	fragments map[int]int
}

func NewNodeData(id uint64) *NodeData {
	return &NodeData{
		id:        id,
		nodeType:  UNKNOWN,
		edges:     []utils.Edge{},
		parent:    nil,
		children:  []*NodeData{},
		fragments: make(map[int]int),
	}
}

func (node NodeData) String() string {
	childrenData := []uint64{}
	for _, child := range node.children {
		if child == nil {
			log.Println("child is nil")
		}
		childrenData = append(childrenData, child.id)
	}

	parentData := "nil"
	if parent := node.parent; parent != nil {
		parentData = fmt.Sprintf("%d", parent.id)
	}

	return fmt.Sprintf("{id: %d, type: %d, edges: %v, parent: %v, children: %v, fragments: %v}",
		node.id, node.nodeType, node.edges, parentData, childrenData, node.fragments)
}

func (node *NodeData) SetType(nodeType NodeType) {
	node.nodeType = nodeType
}

func (node *NodeData) ClearEdges() {
	node.edges = []utils.Edge{}
}

func (node *NodeData) AddEdges(edges []utils.Edge) {
	node.edges = append(node.edges, edges...)

	for _, edge := range edges {
		node.fragments[edge.Src] = edge.Src
		node.fragments[edge.Dest] = edge.Dest
	}
}

func (node *NodeData) SetParent(parent *NodeData) {
	node.parent = parent
}

func (node *NodeData) SetChildren(children []*NodeData) {
	node.children = children
}

type NodeDataGenerator struct {
	idCounterMutex sync.Mutex
	idCounter      uint64
}

func NewNodeDataGenerator() *NodeDataGenerator {
	return &NodeDataGenerator{
		idCounter: 0,
	}
}

func (nodeGenerator *NodeDataGenerator) getNextId() (uint64, error) {
	nodeGenerator.idCounterMutex.Lock()
	defer nodeGenerator.idCounterMutex.Unlock()

	id := nodeGenerator.idCounter
	nodeGenerator.idCounter++

	return id, nil
}

func (nodeGenerator *NodeDataGenerator) CreateNode() *NodeData {
	id, err := nodeGenerator.getNextId()
	if err != nil {
		log.Fatalf("[ERROR] failed to get next id: %v", err)
	}

	node := NewNodeData(id)
	return node
}
