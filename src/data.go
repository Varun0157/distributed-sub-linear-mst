package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"slices"
	"sync"

	utils "mst/sublinear/utils"
)

type NodeMetaData struct {
	stateMutex sync.Mutex
	id         int32
	lis        net.Listener
	parents    map[int32]*NodeMetaData
	children   map[int32][]*NodeMetaData
	phase      int32
}

func NewNodeMetaData(id int32, lis net.Listener) *NodeMetaData {
	return &NodeMetaData{
		id:       id,
		lis:      lis,
		parents:  make(map[int32]*NodeMetaData),
		children: make(map[int32][]*NodeMetaData),
		phase:    0,
	}
}

func (md *NodeMetaData) String() string {
	md.stateMutex.Lock()
	defer md.stateMutex.Unlock()

	childrenData := make(map[int32][]int32)
	for fragment, children := range md.children {
		for _, child := range children {
			childrenData[fragment] = append(childrenData[fragment], child.id)
		}
	}

	parentData := make(map[int32]int32)
	for fragment, parent := range md.parents {
		parentData[fragment] = parent.id
	}

	addr := md.lis.Addr().String()

	return fmt.Sprintf("{id:%d, addr:%s, children:%v, parents:%v}", md.id, addr, childrenData, parentData)
}

func (md *NodeMetaData) progressPhase() {
	md.stateMutex.Lock()
	defer md.stateMutex.Unlock()

	md.phase++
}

func (md *NodeMetaData) GetAddr() string {
	md.stateMutex.Lock()
	defer md.stateMutex.Unlock()

	return md.lis.Addr().String()
}

func (md *NodeMetaData) SetParent(fragment int32, parent *NodeMetaData) {
	md.stateMutex.Lock()
	defer md.stateMutex.Unlock()

	md.parents[fragment] = parent
}

func (md *NodeMetaData) SetChild(fragment int32, child *NodeMetaData) {
	md.stateMutex.Lock()
	defer md.stateMutex.Unlock()

	md.children[fragment] = append(md.children[fragment], child)
}

func (md *NodeMetaData) isLeaf() bool {
	md.stateMutex.Lock()
	defer md.stateMutex.Unlock()

	return len(md.children) == 0
}

func (md *NodeMetaData) isRoot() bool {
	md.stateMutex.Lock()
	defer md.stateMutex.Unlock()

	return len(md.parents) == 0
}

type NodeData struct {
	// id, addr, md of neighbours
	md *NodeMetaData

	edgesMutex     sync.Mutex
	edges          []*utils.Edge
	updateMutex    sync.Mutex
	update         map[int32]int32
	fragmentsMutex sync.Mutex
	fragments      map[int32]int32 // vertex -> fragment id
	ownedFrags     []int           // responsible for these fragments

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
	node.edgesMutex.Lock()
	defer node.edgesMutex.Unlock()

	node.fragmentsMutex.Lock()
	defer node.fragmentsMutex.Unlock()

	edgeData := make([]utils.Edge, 0)
	for _, edge := range node.edges {
		edgeData = append(edgeData, *edge)
	}

	return fmt.Sprintf("{metadata: %v, edges: %v, fragments: %v, ownedFragments: %v}",
		node.md, edgeData, node.fragments, node.ownedFrags)
}

func (node *NodeData) setUpdate(update map[int32]int32) {
	node.updateMutex.Lock()
	defer node.updateMutex.Unlock()

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

func (node *NodeData) GetFragments() []int {
	fragments := []int{}

	node.fragmentsMutex.Lock()
	defer node.fragmentsMutex.Unlock()

	for _, fragment := range node.fragments {
		for _, fr := range fragments {
			if fr == int(fragment) {
				continue
			}
		}
		fragments = append(fragments, int(fragment))
	}

	return fragments
}

func (node *NodeData) ownFragment(fragment int) {
	node.fragmentsMutex.Lock()
	defer node.fragmentsMutex.Unlock()

	if slices.Contains(node.ownedFrags, fragment) {
		return
	}
	node.ownedFrags = append(node.ownedFrags, fragment)
}

func (node *NodeData) getOwnedFragments() []int {
	node.fragmentsMutex.Lock()
	defer node.fragmentsMutex.Unlock()

	return node.ownedFrags
}

func (node *NodeData) disownAllFragments() {
	node.fragmentsMutex.Lock()
	defer node.fragmentsMutex.Unlock()

	node.ownedFrags = []int{}
}

func (node *NodeData) ownsFragment(fragment int) bool {
	node.fragmentsMutex.Lock()
	defer node.fragmentsMutex.Unlock()

	return slices.Contains(node.ownedFrags, fragment)
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

func (nodeGenerator *NodeDataGenerator) getNextID() (int32, error) {
	nodeGenerator.idCounterMutex.Lock()
	defer nodeGenerator.idCounterMutex.Unlock()

	id := nodeGenerator.idCounter
	nodeGenerator.idCounter++

	return id, nil
}

func (nodeGenerator *NodeDataGenerator) CreateNode() (*NodeData, error) {
	id, err := nodeGenerator.getNextID()
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
