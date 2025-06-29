package utils

type EdgeTarget struct {
	v      int32
	Weight int32
}

func CreateAdjacencyList(edges []*Edge) map[int32][]EdgeTarget {
	adjacencyList := make(map[int32][]EdgeTarget)
	for _, edge := range edges {
		adjacencyList[edge.U] = append(adjacencyList[edge.U], EdgeTarget{v: edge.V, Weight: edge.Weight})
		adjacencyList[edge.V] = append(adjacencyList[edge.V], EdgeTarget{v: edge.U, Weight: edge.Weight})
	}

	return adjacencyList
}

func getMinOutgoingEdge(src int32, targets []EdgeTarget, fragmentIds map[int32]int32) *Edge {
	var minEdge *Edge = nil

	for _, target := range targets {
		if fragmentIds[src] == fragmentIds[target.v] {
			continue
		}
		if minEdge != nil && minEdge.Weight <= target.Weight {
			continue
		}
		minEdge = NewEdge(src, target.v, target.Weight)
	}

	return minEdge
}

// returns the minimum outgoing edge for each fragment given the current
// graph and fragment ids
func GetMoEs(adjacencyList map[int32][]EdgeTarget, fragmentIds map[int32]int32) []*Edge {
	fragToMoe := make(map[int32]*Edge)
	for src, targets := range adjacencyList {
		minEdge := getMinOutgoingEdge(src, targets, fragmentIds)
		if minEdge == nil {
			continue
		}

		fragment := fragmentIds[minEdge.U]
		if currMin, ok := fragToMoe[fragment]; !ok || currMin.Weight > minEdge.Weight {
			fragToMoe[fragment] = minEdge
		}
	}

	moes := make([]*Edge, 0)
	for _, moe := range fragToMoe {
		moes = append(moes, moe)
	}

	return moes
}

func GetEdgeList(adjacencyList map[int32][]EdgeTarget) []*Edge {
	edges := make([]*Edge, 0)
	for src, targets := range adjacencyList {
		for _, target := range targets {
			edges = append(edges, NewEdge(src, target.v, target.Weight))
		}
	}
	return edges
}
