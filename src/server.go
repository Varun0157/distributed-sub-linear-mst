package main

import (
	"context"
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
		s.nodeData.AddFragment(int(node), int(fragment))
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

	// TODO: should we return all moes?
	updatesMap := make(map[int32]int32)
	updatesMap[int32(moe.Src)] = int32(moe.Dest)
	update := &comms.Update{Updates: updatesMap}

	return update, nil
}

func (s *SubLinearServer) upwardPropListener() {
	// wait for all children to send data
	s.nodeData.childReqWg.Wait()
	log.Printf("STATE AFTER GETTING CHILD UPDATE: %s", s.nodeData.String())

	// upward prop
	update, error := func() (*comms.Update, error) {
		if s.nodeData.parent != nil {
			return s.getMoeUpdate()
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
	go s.upwardPropListener()
}

// --- RPCs ---

func (s *SubLinearServer) PropogateUp(ctx context.Context, data *comms.Edges) (*comms.Update, error) {
	// update state with received data
	s.updateState(data.GetEdges(), data.GetFragmentIds())

	// received an update from a child
	log.Printf("%d - received edges from child", s.nodeData.id)
	s.nodeData.childReqWg.Done()

	// wait until update is sent (consumer)
	s.nodeData.updateCond.L.Lock()
	s.nodeData.updateCond.Wait()
	s.nodeData.updateCond.L.Unlock()

	log.Printf("%d - received update", s.nodeData.id)

	resp := &comms.Update{Updates: s.nodeData.update}

	return resp, nil
}
