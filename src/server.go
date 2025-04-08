package main

import (
	"context"
	"log"
	"math"
	edgeDataComms "mst/sublinear/comms"
	utils "mst/sublinear/utils"
	"net"

	"google.golang.org/grpc"
)

type SubLinearServer struct {
	receivedData int // during upward propogation, number of children we received edges from
	nodeData     *NodeData

	addr       string
	grpcServer *grpc.Server
	edgeDataComms.UnimplementedEdgeDataServiceServer
}

func NewSubLinearServer(lis net.Listener, nodeData *NodeData) (*SubLinearServer, error) {
	s := &SubLinearServer{
		receivedData: 0,
		nodeData:     nodeData,
		addr:         lis.Addr().String(),
		grpcServer:   grpc.NewServer(grpc.MaxSendMsgSize(math.MaxInt64), grpc.MaxRecvMsgSize(math.MaxInt64)),
	}

	edgeDataComms.RegisterEdgeDataServiceServer(s.grpcServer, s)
	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			log.Fatalf("%s - failed to serve: %v", s.addr, err)
		}
	}()
	log.Printf("%s - server started", s.addr)

	return s, nil
}

func (s *SubLinearServer) ShutDown() {
	s.grpcServer.GracefulStop()
	log.Printf("%s - server stopped", s.addr)
}

// --- rpcs ---

func (s *SubLinearServer) PropogateUp(ctx context.Context, data *edgeDataComms.AccumulatedData) (*edgeDataComms.DataResponse, error) {
	// add edges from child
	edges := []utils.Edge{}
	for _, edgeData := range data.Edges {
		edge := utils.NewEdge(int(edgeData.GetSrc()), int(edgeData.GetDest()), int(edgeData.GetWeight()))
		edges = append(edges, *edge)
	}
	s.nodeData.AddEdges(edges)

	// add fragments from child
	for vertex, id := range data.GetFragmentIds() {
		s.nodeData.AddFragment(int(vertex), int(id))
	}

	// update received count
	s.receivedData++

	// if we have not received data from all children, return
	if s.receivedData < len(s.nodeData.children) {
		return &edgeDataComms.DataResponse{Success: true}, nil
	}

	// TODO: propogate further up

	return &edgeDataComms.DataResponse{Success: true}, nil
}

func (s *SubLinearServer) PropogateDown(ctx context.Context, data *edgeDataComms.AccumulatedData) (*edgeDataComms.DataResponse, error) {
	return nil, nil
}
