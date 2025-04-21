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

	update := &comms.Update{From: int32(moe.Src), To: int32(moe.Dest)}

	return update, nil
}

func (s *SubLinearServer) upwardPropListener() {
	// wait for all children to send data
	s.nodeData.childReqWg.Wait()
	log.Printf("STATE AFTER GETTING CHILD UPDATE: %s", s.nodeData.String())

	// upward prop
	if s.nodeData.parent != nil {
		update, error := s.sendEdgesUp()
		if error != nil {
			log.Fatalf("failed to send edges up: %v", error)
		}

		updateMap := make(map[int]int)
		updateMap[int(update.GetFrom())] = int(update.GetTo())
		s.nodeData.setUpdate(updateMap)

		for range len(s.nodeData.children) {
			s.nodeData.updateWg.Done()
		}
	} else {
		update, error := s.getMoeUpdate()
		if error != nil {
			log.Printf("failed to get update: %v", error)
		}
		log.Printf("update: %v", update)

		updateMap := make(map[int]int)
		updateMap[int(update.GetFrom())] = int(update.GetTo())

		s.nodeData.setUpdate(updateMap)

		for range len(s.nodeData.children) {
			s.nodeData.updateWg.Done()
		}
	}

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

	// another child requesting an update
	s.nodeData.updateWg.Add(1)
	s.nodeData.updateWg.Wait()
	log.Printf("%d - received update", s.nodeData.id)

	var from *int = nil
	var to *int = nil
	for k, v := range s.nodeData.update {
		from = &k
		to = &v
	}

	if from == nil || to == nil {
		log.Fatalf("no update found")
	}

	resp := &comms.Update{From: int32(*from), To: int32(*to)}

	return resp, nil
}
