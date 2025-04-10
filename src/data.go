package main

import (
	"fmt"
	"log"
	"net"
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
	// static after init
	id       uint64
	lis      net.Listener
	nodeType NodeType
	parent   *NodeData
	children []*NodeData

	// dynamic with phase progression
	edgesMutex     sync.Mutex
	edges          []utils.Edge
	fragmentsMutex sync.Mutex
	fragments      map[int]int
}

func NewNodeData(id uint64, lis net.Listener) *NodeData {
	return &NodeData{
		id:        id,
		lis:       lis,
		nodeType:  UNKNOWN,
		edges:     []utils.Edge{},
		parent:    nil,
		children:  []*NodeData{},
		fragments: make(map[int]int),
	}
}

func (node *NodeData) String() string {
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

	return fmt.Sprintf("{id: %d, addr: %s, type: %d, edges: %v, parent: %v, children: %v, fragments: %v}",
		node.id, node.GetAddr(), node.nodeType, node.edges, parentData, childrenData, node.fragments)
}

func (node *NodeData) GetAddr() string {
	return node.lis.Addr().String()
}

func (node *NodeData) SetType(nodeType NodeType) {
	node.nodeType = nodeType
}

func (node *NodeData) SetParent(parent *NodeData) {
	node.parent = parent
}

func (node *NodeData) SetChildren(children []*NodeData) {
	node.children = children
}

func (node *NodeData) ClearEdges() {
	node.edgesMutex.Lock()
	defer node.edgesMutex.Unlock()

	node.edges = []utils.Edge{}
}

func (node *NodeData) AddEdges(edges []utils.Edge) {
	node.edgesMutex.Lock()
	defer node.edgesMutex.Unlock()

	node.edges = append(node.edges, edges...)
}

func (node *NodeData) AddFragment(vertex, id int) {
	node.fragmentsMutex.Lock()
	defer node.fragmentsMutex.Unlock()

	node.fragments[vertex] = id
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

func (nodeGenerator *NodeDataGenerator) CreateNode(lis net.Listener) *NodeData {
	id, err := nodeGenerator.getNextId()
	if err != nil {
		log.Fatalf("[ERROR] failed to get next id: %v", err)
	}

	node := NewNodeData(id, lis)
	return node
}
