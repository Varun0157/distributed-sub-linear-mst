package main

import (
	"context"
	"fmt"
	"log"
	"math"
	comms "mst/sublinear/comms"
	utils "mst/sublinear/utils"

	"google.golang.org/grpc"
)

type SubLinearServer struct {
	receivedCount int // during upward propogation, number of children we received edges from
	nodeData      *NodeData

	grpcServer *grpc.Server
	comms.UnimplementedEdgeDataServiceServer
}

func NewSubLinearServer(nodeData *NodeData) (*SubLinearServer, error) {
	s := &SubLinearServer{
		receivedCount: 0,
		nodeData:      nodeData,
		grpcServer:    grpc.NewServer(grpc.MaxSendMsgSize(math.MaxInt64), grpc.MaxRecvMsgSize(math.MaxInt64)),
	}

	comms.RegisterEdgeDataServiceServer(s.grpcServer, s)
	go func() {
		// TODO: consider removing the Fatalf and returning an error instead would involve channels and such
		if err := s.grpcServer.Serve(s.nodeData.lis); err != nil {
			log.Fatalf("%s - failed to serve: %v", s.nodeData.GetAddr(), err)
		}
	}()
	log.Printf("%s - server started", s.nodeData.GetAddr())

	return s, nil
}

func (s *SubLinearServer) ShutDown() {
	s.grpcServer.GracefulStop()
	log.Printf("%s - server stopped", s.nodeData.GetAddr())
}

func (s *SubLinearServer) updateState(edgeData []*comms.EdgeData, fragmentIds map[int32]int32) {
	// add edges from request
	edges := []*utils.Edge{}
	for _, edgeData := range edgeData {
		src := int(edgeData.GetSrc())
		dest := int(edgeData.GetDest())
		weight := int(edgeData.GetWeight())

		edge := utils.NewEdge(src, dest, weight)
		edges = append(edges, edge)
	}
	s.nodeData.AddEdges(edges)

	// mark the fragments the nodes belong to
	for node, fragment := range fragmentIds {
		s.nodeData.UpdateFragment(int(node), int(fragment))
	}
}

func (s *SubLinearServer) getMoeUpdate() (*comms.Update, error) {
	adjacencyList := utils.CreateAdjacencyList(s.nodeData.edges)
	moes := utils.GetMoEs(adjacencyList, s.nodeData.fragments)

	var moe *utils.Edge = nil
	for _, edge := range moes {
		if moe != nil && moe.Weight < edge.Weight {
			continue
		}
		moe = edge
	}

	updatesMap := make(map[int32]int32)
	if moe != nil {
		log.Printf("selecting %v as moe", moe)

		// TODO: should we return all moes?
		srcFragment := int32(s.nodeData.fragments[int(moe.Src)])
		trgFragment := int32(s.nodeData.fragments[int(moe.Dest)])
		updatesMap[srcFragment] = trgFragment
	}

	update := &comms.Update{Updates: updatesMap}

	return update, nil
}

// TODO: consider making this a driver for non child nodes
// if no more children, return and shutdown outside
func (s *SubLinearServer) nonLeafDriver() {
	for len(s.nodeData.children) > 0 {
		// wait for all children to send data
		s.nodeData.childReqWg.Wait()
		log.Printf("STATE AFTER GETTING CHILD UPDATE: %s", s.nodeData.String())

		// upward prop
		update, error := func() (*comms.Update, error) {
			if s.nodeData.parent != nil {
				edges, fragments := s.getEdgesToSend()
				return s.sendEdgesUp(edges, fragments)
			} else {
				return s.getMoeUpdate()
			}
		}()
		if error != nil {
			log.Fatalf("failed to send edges up: %v", error)
		}

		s.nodeData.setUpdate(update.GetUpdates())

		// wake consumers of update so they can send a response to children
		s.nodeData.updateCond.Broadcast()

		// launch another listener
		s.nodeData.childReqWg.Add(len(s.nodeData.children))
	}
}

func (s *SubLinearServer) leafDriver() error {
	if !s.nodeData.isLeaf() || s.nodeData.parent == nil {
		return fmt.Errorf("leaf driver called on non-leaf node")
	}

	for {
		edges, fragments := s.getEdgesToSend()
		update, err := s.sendEdgesUp(edges, fragments)
		if err != nil {
			return fmt.Errorf("failed to send edges up: %v", err)
		}

		for srcFrag, trgFrag := range update.GetUpdates() {
			for node, frag := range s.nodeData.fragments {
				if frag != int(srcFrag) {
					continue
				}
				s.nodeData.UpdateFragment(node, int(trgFrag))
			}
		}

		// if we did not send any edges in the last update, break
		if len(edges) == 0 {
			break
		}
	}

	return nil
}

// --- RPCs ---

func (s *SubLinearServer) PropogateUp(ctx context.Context, data *comms.Edges) (*comms.Update, error) {
	// update state with received data
	s.updateState(data.GetEdges(), data.GetFragmentIds())
	if len(data.GetEdges()) < 1 && len(data.GetFragmentIds()) < 1 {
		// remove the child from further consideration (in further rounds)
		s.nodeData.RemoveChild(uint64(data.GetSrcId()))
	}

	// received an update from a child
	log.Printf("%d - received edges from child", s.nodeData.id)
	s.nodeData.childReqWg.Done()

	// wait until update is set (consumer of Cond)
	s.nodeData.updateCond.L.Lock()
	s.nodeData.updateCond.Wait()
	s.nodeData.updateCond.L.Unlock()

	// delete current store of edges and fragments
	s.nodeData.ClearEdges()
	s.nodeData.ClearFragments()

	// propogate update down
	log.Printf("%d - received update %v", s.nodeData.id, s.nodeData.update)
	resp := &comms.Update{Updates: s.nodeData.update}

	return resp, nil
}
