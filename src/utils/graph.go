package utils

type EdgeTarget struct {
	Dest   int
	Weight int
}

func CreateAdjacencyList(edges []*Edge) map[int][]EdgeTarget {
	adjacencyList := make(map[int][]EdgeTarget)
	for _, edge := range edges {
		adjacencyList[edge.Src] = append(adjacencyList[edge.Src], EdgeTarget{Dest: edge.Dest, Weight: edge.Weight})
	}

	return adjacencyList
}

// getMoE returns the minimum outgoing edge for each fragment given the current
// graph and fragment ids
func getMoE(adjacencyList map[int][]EdgeTarget, fragmentIds map[int]int) map[int]*Edge {
	getMinOutgoingTargetEdge := func(src int, targets []EdgeTarget) *Edge {
		var minEdge *Edge = nil

		for _, target := range targets {
			if fragmentIds[src] == fragmentIds[target.Dest] {
				continue
			}
			if minEdge != nil && minEdge.Weight <= target.Weight {
				continue
			}
			minEdge = NewEdge(src, target.Dest, target.Weight)
		}

		return minEdge
	}

	moes := make(map[int]*Edge)
	for src, targets := range adjacencyList {
		minEdge := getMinOutgoingTargetEdge(src, targets)
		if minEdge == nil {
			continue
		}

		fragment := fragmentIds[minEdge.Src]
		if currMin, ok := moes[fragment]; !ok || currMin.Weight > minEdge.Weight {
			moes[fragment] = minEdge
		}
	}

	return moes
}

func GetEdgeList(adjacencyList map[int][]EdgeTarget) []*Edge {
	edges := make([]*Edge, 0)
	for src, targets := range adjacencyList {
		for _, target := range targets {
			edges = append(edges, NewEdge(src, target.Dest, target.Weight))
		}
	}
	return edges
}
