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

type Node struct {
	id       uint64
	nodeType NodeType

	edges    []utils.Edge
	parent   *Node
	children []*Node
}

func NewNode(id uint64) *Node {
	return &Node{
		id:       id,
		nodeType: UNKNOWN,
		edges:    []utils.Edge{},
		parent:   nil,
		children: []*Node{},
	}
}

func (node Node) String() string {
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

	return fmt.Sprintf("{id: %d, type: %d, edges: %v, parent: %v, children: %v}", node.id, node.nodeType, node.edges, parentData, childrenData)
}

func (node *Node) SetType(nodeType NodeType) {
	node.nodeType = nodeType
}

func (node *Node) ClearEdges() {
	node.edges = []utils.Edge{}
}

func (node *Node) AddEdges(edges []utils.Edge) {
	node.edges = append(node.edges, edges...)
}

func (node *Node) SetParent(parent *Node) {
	node.parent = parent
}

func (node *Node) SetChildren(children []*Node) {
	node.children = children
}

type NodeGenerator struct {
	idCounterMutex sync.Mutex
	idCounter      uint64
}

func NewNodeGenerator() *NodeGenerator {
	return &NodeGenerator{
		idCounter: 0,
	}
}

func (nodeGenerator *NodeGenerator) getNextId() (uint64, error) {
	nodeGenerator.idCounterMutex.Lock()
	defer nodeGenerator.idCounterMutex.Unlock()

	id := nodeGenerator.idCounter
	nodeGenerator.idCounter++

	return id, nil
}

func (nodeGenerator *NodeGenerator) CreateNode() *Node {
	id, err := nodeGenerator.getNextId()
	if err != nil {
		log.Fatalf("[ERROR] failed to get next id: %v", err)
	}

	node := NewNode(id)
	return node
}
