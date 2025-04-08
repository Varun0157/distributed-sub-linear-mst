package main

import (
	"context"
	"log"
	"math"
	edgeDataComms "mst/sublinear/comms"
	"net"

	"google.golang.org/grpc"
)

type SubLinearServer struct {
	nodeData *NodeData

	addr       string
	grpcServer *grpc.Server
	edgeDataComms.UnimplementedEdgeDataServiceServer
}

func NewSubLinearServer(lis net.Listener, nodeData *NodeData) (*SubLinearServer, error) {
	s := &SubLinearServer{
		nodeData:   nodeData,
		addr:       lis.Addr().String(),
		grpcServer: grpc.NewServer(grpc.MaxSendMsgSize(math.MaxInt64), grpc.MaxRecvMsgSize(math.MaxInt64)),
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

// rpcs
func (s *SubLinearServer) PropogateUp(ctx context.Context, data *edgeDataComms.AccumulatedData) (*edgeDataComms.DataResponse, error) {
	return nil, nil
}

func (s *SubLinearServer) PropogateDown(ctx context.Context, data *edgeDataComms.AccumulatedData) (*edgeDataComms.DataResponse, error) {
	return nil, nil
}
