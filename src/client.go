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

func (s *SubLinearServer) sendEdgesUp() (*comms.Update, error) {
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

	adjacencyList := utils.CreateAdjacencyList(s.nodeData.edges)
	moes := utils.GetMoEs(adjacencyList, s.nodeData.fragments)

	moeData := make([]*comms.EdgeData, 0)
	fragmentData := make(map[int32]int32)
	for _, edge := range moes {
		moeData = append(moeData, &comms.EdgeData{
			Src:    int32(edge.Src),
			Dest:   int32(edge.Dest),
			Weight: int32(edge.Weight),
		})

		for _, vertex := range []int{int(edge.Src), int(edge.Dest)} {
			fragmentData[int32(vertex)] = int32(s.nodeData.fragments[vertex])
		}
	}
	log.Printf("%d - sending %v edges and %v fragments to %d", s.nodeData.id, moeData, fragmentData, s.nodeData.parent.id)

	req := &comms.Edges{Edges: moeData, FragmentIds: fragmentData}

	ctx, cancel := context.WithTimeout(context.Background(), utils.RpcTimeout())
	defer cancel()

	update, err := client.PropogateUp(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to send edge data: %v", err)
	}

	return update, nil
}
