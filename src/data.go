package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"

	utils "mst/sublinear/utils"
)

type NodeType int

type NodeData struct {
	// TODO: make the parent only store id and addr of child
	// 	and vice versa
	// static after init
	id       uint64
	lis      net.Listener
	parent   *NodeData
	children []*NodeData

	// dynamic with phase progression
	edgesMutex     sync.Mutex
	edges          []*utils.Edge
	update         map[int32]int32
	fragmentsMutex sync.Mutex
	fragments      map[int]int

	// for tracking child requests
	childReqWg sync.WaitGroup
	updateCond sync.Cond
}

func NewNodeData(id uint64, lis net.Listener) *NodeData {
	return &NodeData{
		id:         id,
		lis:        lis,
		parent:     nil,
		children:   []*NodeData{},
		edges:      []*utils.Edge{},
		update:     make(map[int32]int32),
		fragments:  make(map[int]int),
		updateCond: *sync.NewCond(&sync.Mutex{}),
	}
}

func (node *NodeData) String() string {
	childrenData := []uint64{}
	for _, child := range node.children {
		if child == nil {
			continue
		}
		childrenData = append(childrenData, child.id)
	}

	parentData := "nil"
	if parent := node.parent; parent != nil {
		parentData = fmt.Sprintf("%d", parent.id)
	}

	edgeData := make([]utils.Edge, 0)
	for _, edge := range node.edges {
		edgeData = append(edgeData, *edge)
	}

	return fmt.Sprintf("{id: %d, addr: %s, edges: %v, parent: %v, children: %v, fragments: %v}",
		node.id, node.GetAddr(), edgeData, parentData, childrenData, node.fragments)
}

func (node *NodeData) setUpdate(update map[int32]int32) {
	node.update = update
}

func (node *NodeData) GetAddr() string {
	return node.lis.Addr().String()
}

func (node *NodeData) SetParent(parent *NodeData) {
	node.parent = parent
}

func (node *NodeData) RemoveChild(childId uint64) {
	for i, child := range node.children {
		if child.id != childId {
			continue
		}
		node.children = append(node.children[:i], node.children[i+1:]...)
		break
	}
}

func (node *NodeData) isLeaf() bool {
	return len(node.children) == 0 && node.parent != nil
}

func (node *NodeData) isRoot() bool {
	return node.parent == nil
}

func (node *NodeData) SetChildren(children []*NodeData) {
	node.children = children
}

func (node *NodeData) ClearEdges() {
	node.edgesMutex.Lock()
	defer node.edgesMutex.Unlock()

	node.edges = []*utils.Edge{}
}

func (node *NodeData) AddEdges(edges []*utils.Edge) {
	node.edgesMutex.Lock()
	defer node.edgesMutex.Unlock()

	node.edges = append(node.edges, edges...)
}

func (node *NodeData) ClearFragments() {
	node.fragmentsMutex.Lock()
	defer node.fragmentsMutex.Unlock()

	node.fragments = make(map[int]int)
}

func (node *NodeData) UpdateFragment(vertex, id int) {
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

func listenOnRandomAddr() (lis net.Listener, err error) {
	for {
		port := rand.Intn(65535-1024) + 1024
		addr := fmt.Sprintf(":%d", port)

		lis, err = net.Listen("tcp", addr)
		if err == nil {
			break
		}

		log.Printf("failed to listen on addr %s: %v", addr, err)
	}
	log.Printf("listening on port %s", lis.Addr().String())

	return lis, nil
}

func (nodeGenerator *NodeDataGenerator) getNextId() (uint64, error) {
	nodeGenerator.idCounterMutex.Lock()
	defer nodeGenerator.idCounterMutex.Unlock()

	id := nodeGenerator.idCounter
	nodeGenerator.idCounter++

	return id, nil
}

func (nodeGenerator *NodeDataGenerator) CreateNode() (*NodeData, error) {
	id, err := nodeGenerator.getNextId()
	if err != nil {
		return nil, fmt.Errorf("failed to get next id: %v", err)
	}

	lis, err := listenOnRandomAddr()
	if err != nil {
		return nil, fmt.Errorf("failed to listen on random addr: %v", err)
	}

	node := NewNodeData(id, lis)
	return node, nil
}
