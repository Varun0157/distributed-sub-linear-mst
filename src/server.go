package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"

	comms "mst/sublinear/comms"
	utils "mst/sublinear/utils"
	"slices"

	"google.golang.org/grpc"
)

type SubLinearServer struct {
	receivedCount int // during upward propogation, number of children we received edges from
	nodeData      *NodeData
	outFile       string

	grpcServer *grpc.Server
	comms.UnimplementedEdgeDataServiceServer
}

func NewSubLinearServer(nodeData *NodeData, outFile string) (*SubLinearServer, error) {
	s := &SubLinearServer{
		receivedCount: 0,
		nodeData:      nodeData,
		outFile:       outFile,
		grpcServer:    grpc.NewServer(grpc.MaxSendMsgSize(math.MaxInt64), grpc.MaxRecvMsgSize(math.MaxInt64)),
	}

	comms.RegisterEdgeDataServiceServer(s.grpcServer, s)
	go func() {
		// TODO: consider removing the Fatalf and returning an error instead (would involve channels and such)
		if err := s.grpcServer.Serve(s.nodeData.md.lis); err != nil {
			log.Fatalf("%s - failed to serve: %v", s.nodeData.md.GetAddr(), err)
		}
	}()
	log.Printf("%s - server started", s.nodeData.md.GetAddr())

	return s, nil
}

func (s *SubLinearServer) ShutDown() {
	s.grpcServer.GracefulStop()
	log.Printf("%s - server stopped", s.nodeData.md.GetAddr())
}

func (s *SubLinearServer) updateState(edgeData []*comms.EdgeData, fragmentIds map[int32]int32) {
	// add edges from request
	edges := []*utils.Edge{}
	for _, edgeData := range edgeData {
		src := edgeData.GetU()
		dest := edgeData.GetV()
		weight := edgeData.GetWeight()

		edge := utils.NewEdge(src, dest, weight)
		edges = append(edges, edge)
	}
	s.nodeData.AddEdges(edges)

	// mark the fragments the nodes belong to
	for node, fragment := range fragmentIds {
		s.nodeData.UpdateFragment(node, fragment)
	}
}

func (s *SubLinearServer) getMoeUpdate() (*comms.Update, error) {
	adjacencyList := utils.CreateAdjacencyList(s.nodeData.edges)
	moes := utils.GetMoEs(adjacencyList, s.nodeData.fragments)
	log.Printf("-----> %v are moes", moes)

	updatesMap := make(map[int32]int32)

	for _, edge := range moes {
		srcFragment := int32(s.nodeData.fragments[edge.U])
		trgFragment := int32(s.nodeData.fragments[edge.V])

		// until the introduction of red-blue randomness
		newFrags := func() bool {
			for src, trg := range updatesMap {
				nodes := []int32{srcFragment, trgFragment}
				if slices.Contains(nodes, src) || slices.Contains(nodes, trg) {
					return false
				}
			}
			return true
		}()
		if !newFrags {
			continue
		}

		updatesMap[srcFragment] = trgFragment
		utils.WriteGraph(s.outFile, []*utils.Edge{edge})
	}

	update := &comms.Update{Updates: updatesMap}

	return update, nil
}

func (s *SubLinearServer) nonLeafDriver() error {
	// while we have children
	for s.nodeData.md.GetTotalChildren() > 0 {
		// wait for the moes from all the children
		totalChildren := s.nodeData.md.GetTotalChildren()
		s.nodeData.childReqWg.Add(totalChildren)
		s.nodeData.childReqWg.Wait()

		log.Printf("STATE AFTER GETTING CHILD UPDATE: %s", s.nodeData.String())

		// upward prop
		update, error := func() (*comms.Update, error) {
			if s.nodeData.md.HasParents() {
				noMoreUpdates, edgesByFragment, fragmentsByFragment := s.getEdgesToSend()

				allUpdates := make(map[int32]int32)
				var updateMutex sync.Mutex

				// Send to every parent for every fragment relationship concurrently
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

						fragmentUpdate, err := s.sendEdgesUpForFragment(fragID, fragmentNoMoreUpdates, edges, fragments)
						if err != nil {
							errChan <- fmt.Errorf("failed to send edges up for fragment %d: %v", fragID, err)
							return
						}

						updateMutex.Lock()
						for srcFrag, trgFrag := range fragmentUpdate.GetUpdates() {
							allUpdates[srcFrag] = trgFrag
						}
						updateMutex.Unlock()
					}()
				}

				wg.Wait()
				close(errChan)

				if err := <-errChan; err != nil {
					return nil, err
				}

				return &comms.Update{Updates: allUpdates}, nil
			} else {
				return s.getMoeUpdate()
			}
		}()
		if error != nil {
			return fmt.Errorf("failed to send edges up: %v", error)
		}

		// delete current store of edges and fragments
		s.nodeData.ClearEdges()
		s.nodeData.ClearFragments()

		// set the update and wake the consumers (handlers of RPC calls from children)
		s.nodeData.setUpdate(update.GetUpdates())
		s.nodeData.updateCond.Broadcast()

		// progress the phase counter
		s.nodeData.md.progressPhase()
	}

	return nil
}

// --- RPC ---

func (s *SubLinearServer) PropogateUp(ctx context.Context, data *comms.Edges) (*comms.Update, error) {
	// update state with received data
	s.updateState(data.GetEdges(), data.GetFragmentIds())

	// Note: We don't remove children during execution to avoid WaitGroup race conditions
	// Children with noMoreUpdates will continue sending empty messages in subsequent rounds

	// received an update from a child
	log.Printf("%d - received edges from child", s.nodeData.md.id)
	s.nodeData.childReqWg.Done()

	// wait until update is set (consumer of Cond)
	s.nodeData.updateCond.L.Lock()
	s.nodeData.updateCond.Wait()
	s.nodeData.updateCond.L.Unlock()

	// propogate update down
	log.Printf("%d - received update %v", s.nodeData.md.id, s.nodeData.update)
	resp := &comms.Update{Updates: s.nodeData.update}

	return resp, nil
}
