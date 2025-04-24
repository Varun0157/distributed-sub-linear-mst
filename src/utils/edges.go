package utils

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

type Edge struct {
	U      int32
	V      int32
	Weight int32
}

func (Edge *Edge) String() string {
	return fmt.Sprintf("src: %d, dest: %d, weight: %d", Edge.U, Edge.V, Edge.Weight)
}

func NewEdge(src, dest, weight int32) *Edge {
	return &Edge{
		U:      src,
		V:      dest,
		Weight: weight,
	}
}

func GetNumberOfVertices(edges []Edge) (int, error) {
	uniqueVertices := make(map[int32]bool)

	for _, edge := range edges {
		uniqueVertices[edge.U] = true
		uniqueVertices[edge.V] = true
	}

	return len(uniqueVertices), nil
}

func getMaxVertex(edges []Edge) (int, error) {
	if len(edges) < 1 {
		return 0, fmt.Errorf("no edges provided")
	}

	maxVertex := 0
	for _, edge := range edges {
		maxVertex = int(math.Max(float64(maxVertex), float64(edge.U)))
		maxVertex = int(math.Max(float64(maxVertex), float64(edge.V)))
	}

	return maxVertex, nil
}

func GetStats(edges []*Edge) (int, int, int) {
	uniqueVertices := make(map[int32]bool)
	weight := 0
	for _, edge := range edges {
		for _, vertex := range []int32{edge.U, edge.V} {
			uniqueVertices[vertex] = true
		}
		weight += int(edge.Weight)
	}

	numVertices := len(uniqueVertices)
	numEdges := len(edges)
	return numVertices, numEdges, weight
}

func ReadGraph(fileName string) ([]*Edge, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var edges []*Edge
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		parts := strings.Fields(scanner.Text())
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid line: %s", scanner.Text())
		}

		src, err1 := strconv.Atoi(parts[0])
		dest, err2 := strconv.Atoi(parts[1])
		weight, err3 := strconv.Atoi(parts[2])

		if err1 != nil || err2 != nil || err3 != nil {
			return nil, fmt.Errorf("invalid line: %s", scanner.Text())
		}

		edges = append(edges, NewEdge(int32(src), int32(dest), int32(weight)))
	}

	if err = scanner.Err(); err != nil {
		return nil, err
	}

	return edges, nil
}

func SortEdges(edges []*Edge) {
	sort.Slice(edges, func(i, j int) bool {
		return (edges[i].Weight < edges[j].Weight) ||
			(edges[i].Weight == edges[j].Weight && edges[i].U < edges[j].U) ||
			(edges[i].Weight == edges[j].Weight && edges[i].U == edges[j].U && edges[i].V < edges[j].V)
	})
}

func WriteGraph(fileName string, edges []*Edge) error {
	SortEdges(edges)

	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, edge := range edges {
		_, err := fmt.Fprintf(writer, "%d %d %d\n", edge.U, edge.V, edge.Weight)
		if err != nil {
			return err
		}
	}

	return writer.Flush()
}
