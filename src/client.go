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

func (s *SubLinearServer) getEdgesToSend() (bool, []*utils.Edge, map[int32]int32) {
	adjacencyList := utils.CreateAdjacencyList(s.nodeData.edges)
	moes := utils.GetMoEs(adjacencyList, s.nodeData.fragments)

	filteredMoes := make([]*utils.Edge, 0)
	sr := NewSharedRandomness()
	round := int(s.nodeData.md.phase)
	for _, edge := range moes {
		uCol := sr.GetFragmentColour(round, int(edge.U))
		vCol := sr.GetFragmentColour(round, int(edge.V))
		if uCol == vCol {
			log.Printf("---> %d and %d have the same colour, skipping", edge.U, edge.V)
		}
		filteredMoes = append(filteredMoes, edge)
	}

	fragments := make(map[int32]int32)
	for _, edge := range filteredMoes {
		for _, vertex := range []int32{edge.U, edge.V} {
			fragments[vertex] = s.nodeData.fragments[vertex]
		}
	}

	noMoreUpdates := len(filteredMoes) == 0
	return noMoreUpdates, filteredMoes, fragments
}

func (s *SubLinearServer) sendEdgesUp(noMoreUpdates bool, edges []*utils.Edge, fragments map[int32]int32) (*comms.Update, error) {
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
			U:      int32(edge.U),
			V:      int32(edge.V),
			Weight: int32(edge.Weight),
		}
	}
	log.Printf("%d - sending %v edges and %v fragments to %d", s.nodeData.md.id, moeData, fragments, s.nodeData.md.parent.id)

	req := &comms.Edges{SrcId: s.nodeData.md.id, NoMoreUpdates: noMoreUpdates, Edges: moeData, FragmentIds: fragments}

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
		allSame, edges, fragments := s.getEdgesToSend()
		update, err := s.sendEdgesUp(allSame, edges, fragments)
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

		s.nodeData.md.progressPhase()

		// if we did not send any edges in the last update, break
		if len(edges) == 0 {
			break
		}
	}

	return nil
}
