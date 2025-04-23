package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"

	utils "mst/sublinear/utils"
)

type NodeMetaData struct {
	id       int32
	lis      net.Listener
	parent   *NodeMetaData
	children []*NodeMetaData
}

func NewNodeMetaData(id int32, lis net.Listener) *NodeMetaData {
	return &NodeMetaData{
		id:       id,
		lis:      lis,
		parent:   nil,
		children: []*NodeMetaData{},
	}
}

func (md *NodeMetaData) String() string {
	childrenData := []int32{}
	for _, child := range md.children {
		if child == nil {
			continue
		}
		childrenData = append(childrenData, child.id)
	}

	parentData := "nil"
	if parent := md.parent; parent != nil {
		parentData = fmt.Sprintf("%d", parent.id)
	}

	return fmt.Sprintf("{id: %d, addr: %s, children: %v, parent: %s}", md.id, md.GetAddr(), childrenData, parentData)
}

func (md *NodeMetaData) GetAddr() string {
	return md.lis.Addr().String()
}

func (md *NodeMetaData) SetParent(parent *NodeMetaData) {
	md.parent = parent
}

func (md *NodeMetaData) RemoveChild(childId int32) {
	for i, child := range md.children {
		if child.id != childId {
			continue
		}
		md.children = append(md.children[:i], md.children[i+1:]...)
		break
	}
}

func (md *NodeMetaData) isLeaf() bool {
	return len(md.children) == 0 && md.parent != nil
}

func (md *NodeMetaData) isRoot() bool {
	return md.parent == nil
}

func (md *NodeMetaData) SetChildren(children []*NodeMetaData) {
	md.children = children
}

type NodeData struct {
	// id, addr, md of neighbours
	md *NodeMetaData

	edgesMutex     sync.Mutex
	edges          []*utils.Edge
	update         map[int32]int32
	fragmentsMutex sync.Mutex
	fragments      map[int32]int32

	// for tracking child requests
	childReqWg sync.WaitGroup
	updateCond sync.Cond
}

func NewNodeData(id int32, lis net.Listener) *NodeData {
	metadata := NewNodeMetaData(id, lis)

	return &NodeData{
		md:         metadata,
		edges:      []*utils.Edge{},
		update:     make(map[int32]int32),
		fragments:  make(map[int32]int32),
		updateCond: *sync.NewCond(&sync.Mutex{}),
	}
}

func (node *NodeData) String() string {
	edgeData := make([]utils.Edge, 0)
	for _, edge := range node.edges {
		edgeData = append(edgeData, *edge)
	}

	return fmt.Sprintf("{metadata: %v, edges: %v, fragments: %v}",
		node.md, edgeData, node.fragments)
}

func (node *NodeData) setUpdate(update map[int32]int32) {
	node.update = update
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

	node.fragments = make(map[int32]int32)
}

func (node *NodeData) UpdateFragment(vertex, id int32) {
	node.fragmentsMutex.Lock()
	defer node.fragmentsMutex.Unlock()

	node.fragments[vertex] = id
}

type NodeDataGenerator struct {
	idCounterMutex sync.Mutex
	idCounter      int32
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

func (nodeGenerator *NodeDataGenerator) getNextId() (int32, error) {
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
