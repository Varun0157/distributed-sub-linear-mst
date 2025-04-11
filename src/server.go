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
	fragmentUpdates := make(map[int]int)

	// add edges from request
	edges := []*utils.Edge{}
	for _, edgeData := range edgeData {
		src := int(edgeData.GetSrc())
		dest := int(edgeData.GetDest())
		weight := int(edgeData.GetWeight())

		edge := utils.NewEdge(src, dest, weight)
		edges = append(edges, edge)

		srcFragment := int(fragmentIds[int32(src)])
		trgFragment := int(fragmentIds[int32(dest)])
		fragmentUpdates[srcFragment] = trgFragment
	}
	s.nodeData.AddEdges(edges)

	for vertex, fragment := range fragmentIds {
		// if we have '1' -> '2' and '2' -> '3', we want to make sure that
		// we ultimately have '1' -> '3' and '2' -> '3'
		oldFragment := int(fragment)

		newFragment := oldFragment
		if _, ok := fragmentUpdates[oldFragment]; ok {
			newFragment = fetchLeafValue(fragmentUpdates, oldFragment)
		}

		s.nodeData.AddFragment(int(vertex), newFragment)
	}

	// update received count
	s.receivedCount++
}

// --- RPCs ---

func (s *SubLinearServer) PropogateUp(ctx context.Context, data *edgeDataComms.AccumulatedData) (*edgeDataComms.DataResponse, error) {
	// update state with received data
	s.updateState(data.GetEdges(), data.GetFragmentIds())

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
