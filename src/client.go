package main

import (
	"context"
	"fmt"
	"log"
	comms "mst/sublinear/comms"
	utils "mst/sublinear/utils"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (s *SubLinearServer) getEdgesToSend() ([]*utils.Edge, map[int32]int32) {
	adjacencyList := utils.CreateAdjacencyList(s.nodeData.edges)
	moes := utils.GetMoEs(adjacencyList, s.nodeData.fragments)

	fragments := make(map[int32]int32)
	for _, edge := range moes {
		for _, vertex := range []int32{edge.Src, edge.Dest} {
			fragments[vertex] = s.nodeData.fragments[vertex]
		}
	}

	return moes, fragments
}

func (s *SubLinearServer) sendEdgesUp(edges []*utils.Edge, fragments map[int32]int32) (*comms.Update, error) {
	if s.nodeData.md.parent == nil {
		return nil, fmt.Errorf("no parent node to send edges to")
	}
	receiverAddr := s.nodeData.md.parent.GetAddr()

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
	log.Printf("%d - sending %v edges and %v fragments to %d", s.nodeData.md.id, moeData, fragments, s.nodeData.md.parent.id)

	req := &comms.Edges{Edges: moeData, FragmentIds: fragments, SrcId: int32(s.nodeData.md.id)}

	ctx, cancel := context.WithTimeout(context.Background(), utils.RpcTimeout())
	defer cancel()

	update, err := client.PropogateUp(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to send edge data: %v", err)
	}

	return update, nil
}

func (s *SubLinearServer) leafDriver() error {
	if !s.nodeData.md.isLeaf() || s.nodeData.md.parent == nil {
		return fmt.Errorf("leaf driver called on non-leaf node")
	}

	for {
		edges, fragments := s.getEdgesToSend()
		update, err := s.sendEdgesUp(edges, fragments)
		if err != nil {
			return fmt.Errorf("failed to send edges up: %v", err)
		}

		// update state of leaf based on update
		for srcFrag, trgFrag := range update.GetUpdates() {
			for node, frag := range s.nodeData.fragments {
				if frag != srcFrag {
					continue
				}
				log.Printf("----> updating node %d from %d to %d", node, frag, trgFrag)
				s.nodeData.UpdateFragment(node, trgFrag)
			}
		}

		time.Sleep(100 * time.Millisecond)

		// if we did not send any edges in the last update, break
		if len(edges) == 0 {
			break
		}
	}

	return nil
}
