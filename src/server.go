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

// --- rpcs ---

func (s *SubLinearServer) PropogateUp(ctx context.Context, data *edgeDataComms.AccumulatedData) (*edgeDataComms.DataResponse, error) {
	// add edges from request
	edges := []utils.Edge{}
	for _, edgeData := range data.Edges {
		edge := utils.NewEdge(int(edgeData.GetSrc()), int(edgeData.GetDest()), int(edgeData.GetWeight()))
		edges = append(edges, *edge)
	}
	s.nodeData.AddEdges(edges)

	// add fragments from request
	for vertex, id := range data.GetFragmentIds() {
		s.nodeData.AddFragment(int(vertex), int(id))
	}

	// update received count
	s.receivedCount++

	// if we have not received data from all children, return
	if s.receivedCount < len(s.nodeData.children) {
		return &edgeDataComms.DataResponse{Success: true}, nil
	}

	// TODO: propogate further up

	return &edgeDataComms.DataResponse{Success: true}, nil
}

func (s *SubLinearServer) PropogateDown(ctx context.Context, data *edgeDataComms.AccumulatedData) (*edgeDataComms.DataResponse, error) {
	return nil, nil
}
