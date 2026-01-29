package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	comms "mst/sublinear/comms"
	utils "mst/sublinear/utils"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (s *SubLinearServer) getEdgesToSend() (bool, map[int32][]*utils.Edge, map[int32]map[int32]int32) {
	adjacencyList := utils.CreateAdjacencyList(s.nodeData.edges)
	moes := utils.GetMoEs(adjacencyList, s.nodeData.fragments)

	filteredMoes := make([]*utils.Edge, 0)
	sr := NewSharedRandomness()
	round := int(s.nodeData.md.phase)
	for _, edge := range moes {
		uFrag := s.nodeData.fragments[edge.U]
		vFrag := s.nodeData.fragments[edge.V]
		uCol := sr.GetFragmentColour(round, int(uFrag))
		vCol := sr.GetFragmentColour(round, int(vFrag))
		if uCol == vCol {
			log.Printf("---> fragments %d and %d have the same colour, skipping edge (%d,%d)", uFrag, vFrag, edge.U, edge.V)
			continue
		}
		filteredMoes = append(filteredMoes, edge)
	}

	edgesByFragment := s.nodeData.groupEdgesByFragment(filteredMoes)

	fragmentsByFragment := make(map[int32]map[int32]int32)
	for fragID, edges := range edgesByFragment {
		fragments := make(map[int32]int32)
		for _, edge := range edges {
			for _, vertex := range []int32{edge.U, edge.V} {
				fragments[vertex] = s.nodeData.fragments[vertex]
			}
		}
		fragmentsByFragment[fragID] = fragments
	}

	noMoreUpdates := len(filteredMoes) == 0
	return noMoreUpdates, edgesByFragment, fragmentsByFragment
}

func (s *SubLinearServer) sendEdgesUpForFragment(fragmentID int32, noMoreUpdates bool, edges []*utils.Edge, fragments map[int32]int32) (*comms.Update, error) {
	parent := s.nodeData.md.parents[fragmentID]
	if parent == nil {
		return nil, fmt.Errorf("no parent node for fragment %d", fragmentID)
	}
	receiverAddr := parent.GetAddr()

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
	log.Printf("%d - sending %v edges and %v fragments for fragment %d to parent %d", s.nodeData.md.id, moeData, fragments, fragmentID, parent.id)

	req := &comms.Edges{SrcId: s.nodeData.md.id, NoMoreUpdates: noMoreUpdates, Edges: moeData, FragmentIds: fragments, FragmentId: fragmentID}

	ctx, cancel := context.WithTimeout(context.Background(), utils.RpcTimeout())
	defer cancel()

	update, err := client.PropogateUp(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to send edge data: %v", err)
	}

	return update, nil
}

func (s *SubLinearServer) leafDriver() error {
	if !s.nodeData.md.isLeaf() || !s.nodeData.md.HasParents() {
		return fmt.Errorf("leaf driver called on non-leaf node or node without parents")
	}

	for {
		noMoreUpdates, edgesByFragment, fragmentsByFragment := s.getEdgesToSend()

		allUpdates := make(map[int32]int32)
		var updateMutex sync.Mutex

		// Send to every parent for every fragment relationship concurrently
		// Parent nodes wait for ALL messages before responding, so we must send in parallel
		var wg sync.WaitGroup
		errChan := make(chan error, len(s.nodeData.md.parents))

		for fragID := range s.nodeData.md.parents {
			fragID := fragID
			edges := edgesByFragment[fragID]
			fragments := fragmentsByFragment[fragID]

			if edges == nil {
				edges = []*utils.Edge{}
			}
			if fragments == nil {
				fragments = make(map[int32]int32)
			}

			fragmentNoMoreUpdates := noMoreUpdates || len(edges) == 0

			wg.Add(1)
			go func() {
				defer wg.Done()

				update, err := s.sendEdgesUpForFragment(fragID, fragmentNoMoreUpdates, edges, fragments)
				if err != nil {
					errChan <- fmt.Errorf("failed to send edges up for fragment %d: %v", fragID, err)
					return
				}

				updateMutex.Lock()
				for srcFrag, trgFrag := range update.GetUpdates() {
					allUpdates[srcFrag] = trgFrag
				}
				updateMutex.Unlock()
			}()
		}

		wg.Wait()
		close(errChan)

		if err := <-errChan; err != nil {
			return err
		}

		for srcFrag, trgFrag := range allUpdates {
			for node, frag := range s.nodeData.fragments {
				if frag != srcFrag {
					continue
				}
				log.Printf("----> updating node %d from %d to %d", node, frag, trgFrag)
				s.nodeData.UpdateFragment(node, trgFrag)
			}
		}

		s.nodeData.md.progressPhase()

		if noMoreUpdates {
			break
		}
	}

	return nil
}
