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

func GetEdgeList(adjacencyList map[int][]EdgeTarget) []*Edge {
	edges := make([]*Edge, 0)
	for src, targets := range adjacencyList {
		for _, target := range targets {
			edges = append(edges, NewEdge(src, target.Dest, target.Weight))
		}
	}
	return edges
}
