package main

import (
	"context"
	"fmt"
	"log"
	comms "mst/sublinear/comms"
	utils "mst/sublinear/utils"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (s *SubLinearServer) getEdgesToSend() ([]*utils.Edge, map[int32]int32) {
	adjacencyList := utils.CreateAdjacencyList(s.nodeData.edges)
	moes := utils.GetMoEs(adjacencyList, s.nodeData.fragments)

	fragments := make(map[int32]int32)
	for _, edge := range moes {
		for _, vertex := range []int{int(edge.Src), int(edge.Dest)} {
			fragments[int32(vertex)] = int32(s.nodeData.fragments[vertex])
		}
	}

	return moes, fragments
}

func (s *SubLinearServer) sendEdgesUp(edges []*utils.Edge, fragments map[int32]int32) (*comms.Update, error) {
	if s.nodeData.parent == nil {
		return nil, fmt.Errorf("no parent node to send edges to")
	}
	receiverAddr := s.nodeData.parent.GetAddr()

	conn, err := grpc.NewClient(receiverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("unable to create client connection: %v", err)
	}
	defer conn.Close()
	client := comms.NewEdgeDataServiceClient(conn)

	moeData := make([]*comms.EdgeData, len(edges))
	for i, edge := range edges {
		moeData[i] = &comms.EdgeData{
			Src:    int32(edge.Src),
			Dest:   int32(edge.Dest),
			Weight: int32(edge.Weight),
		}
	}
	log.Printf("%d - sending %v edges and %v fragments to %d", s.nodeData.id, moeData, fragments, s.nodeData.parent.id)

	req := &comms.Edges{Edges: moeData, FragmentIds: fragments, SrcId: int32(s.nodeData.id)}

	ctx, cancel := context.WithTimeout(context.Background(), utils.RpcTimeout())
	defer cancel()

	update, err := client.PropogateUp(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to send edge data: %v", err)
	}

	return update, nil
}
