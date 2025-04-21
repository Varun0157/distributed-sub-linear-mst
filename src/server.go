package main

import (
	"context"
	"log"
	"math"
	edgeDataComms "mst/sublinear/comms"
	utils "mst/sublinear/utils"

	"google.golang.org/grpc"
)

type SubLinearServer struct {
	receivedCount int // during upward propogation, number of children we received edges from
	nodeData      *NodeData

	grpcServer *grpc.Server
	edgeDataComms.UnimplementedEdgeDataServiceServer
}

func NewSubLinearServer(nodeData *NodeData) (*SubLinearServer, error) {
	s := &SubLinearServer{
		receivedCount: 0,
		nodeData:      nodeData,
		grpcServer:    grpc.NewServer(grpc.MaxSendMsgSize(math.MaxInt64), grpc.MaxRecvMsgSize(math.MaxInt64)),
	}

	edgeDataComms.RegisterEdgeDataServiceServer(s.grpcServer, s)
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

func fetchLeafValue(fragmentIds map[int]int, k int) int {
	v := fragmentIds[k]
	if _, ok := fragmentIds[v]; ok {
		return fetchLeafValue(fragmentIds, v)
	}

	return int(v)
}

func (s *SubLinearServer) updateState(edgeData []*edgeDataComms.EdgeData, fragmentIds map[int32]int32) {
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

// --- RPCs ---

func (s *SubLinearServer) PropogateUp(ctx context.Context, data *edgeDataComms.Edges) (*edgeDataComms.DataResponse, error) {
	// update state with received data
	s.updateState(data.GetEdges(), data.GetFragmentIds())

	// update received count
	s.receivedCount++

	// if we have not received data from all children, return
	if s.receivedCount < len(s.nodeData.children) {
		return &edgeDataComms.DataResponse{Success: true}, nil
	}

	log.Printf("STATE AFTER GETTING CHILD UPDATE: %s", s.nodeData.String())

	// if there is no parent to send data to, return (root node)
	if s.nodeData.parent == nil {
		return &edgeDataComms.DataResponse{Success: true}, nil
	}
	s.sendEdgesUp()

	return &edgeDataComms.DataResponse{Success: true}, nil
}

func (s *SubLinearServer) PropogateDown(ctx context.Context, data *edgeDataComms.AccumulatedData) (*edgeDataComms.DataResponse, error) {
	return nil, nil
}
