package main

import (
	"context"
	"log"
	"math"
	comms "mst/sublinear/comms"
	utils "mst/sublinear/utils"

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
		// TODO: consider removing the Fatalf and returning an error instead would involve channels and such
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
		src := edgeData.GetSrc()
		dest := edgeData.GetDest()
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
	log.Printf("-----> selecting %v as moes", moes)
	utils.WriteGraph(s.outFile, moes)

	updatesMap := make(map[int32]int32)

	for _, edge := range moes {
		srcFragment := int32(s.nodeData.fragments[edge.Src])
		trgFragment := int32(s.nodeData.fragments[edge.Dest])
		updatesMap[srcFragment] = trgFragment
	}

	update := &comms.Update{Updates: updatesMap}

	return update, nil
}

func (s *SubLinearServer) nonLeafDriver() {
	// while we have children
	for len(s.nodeData.md.children) > 0 {
		// wait for all children to send data
		s.nodeData.childReqWg.Wait()
		log.Printf("STATE AFTER GETTING CHILD UPDATE: %s", s.nodeData.String())

		// upward prop
		update, error := func() (*comms.Update, error) {
			if s.nodeData.md.parent != nil {
				edges, fragments := s.getEdgesToSend()
				return s.sendEdgesUp(edges, fragments)
			} else {
				return s.getMoeUpdate()
			}
		}()
		if error != nil {
			log.Fatalf("failed to send edges up: %v", error)
		}

		s.nodeData.setUpdate(update.GetUpdates())

		// wake consumers of update so they can send a response to children
		s.nodeData.updateCond.Broadcast()

		// launch another listener
		s.nodeData.childReqWg.Add(len(s.nodeData.md.children))
	}
}

// --- RPC ---

func (s *SubLinearServer) PropogateUp(ctx context.Context, data *comms.Edges) (*comms.Update, error) {
	// update state with received data
	s.updateState(data.GetEdges(), data.GetFragmentIds())
	if len(data.GetEdges()) < 1 && len(data.GetFragmentIds()) < 1 {
		// remove the child from further consideration (in further rounds)
		s.nodeData.md.RemoveChild(data.GetSrcId())
	}

	// received an update from a child
	log.Printf("%d - received edges from child", s.nodeData.md.id)
	s.nodeData.childReqWg.Done()

	// wait until update is set (consumer of Cond)
	s.nodeData.updateCond.L.Lock()
	s.nodeData.updateCond.Wait()
	s.nodeData.updateCond.L.Unlock()

	// delete current store of edges and fragments
	s.nodeData.ClearEdges()
	s.nodeData.ClearFragments()

	// propogate update down
	log.Printf("%d - received update %v", s.nodeData.md.id, s.nodeData.update)
	resp := &comms.Update{Updates: s.nodeData.update}

	return resp, nil
}
