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
	Src    int
	Dest   int
	Weight int
}

func (Edge *Edge) String() string {
	return fmt.Sprintf("src: %d, dest: %d, weight: %d", Edge.Src, Edge.Dest, Edge.Weight)
}

func NewEdge(src, dest, weight int) *Edge {
	return &Edge{
		Src:    src,
		Dest:   dest,
		Weight: weight,
	}
}

func GetNumberOfVertices(edges []Edge) (int, error) {
	uniqueVertices := make(map[int]bool)

	for _, edge := range edges {
		uniqueVertices[edge.Src] = true
		uniqueVertices[edge.Dest] = true
	}

	return len(uniqueVertices), nil
}

func getMaxVertex(edges []Edge) (int, error) {
	if len(edges) < 1 {
		return 0, fmt.Errorf("no edges provided")
	}

	maxVertex := 0
	for _, edge := range edges {
		maxVertex = int(math.Max(float64(maxVertex), float64(edge.Src)))
		maxVertex = int(math.Max(float64(maxVertex), float64(edge.Dest)))
	}

	return maxVertex, nil
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

		edges = append(edges, NewEdge(src, dest, weight))
	}

	if err = scanner.Err(); err != nil {
		return nil, err
	}

	return edges, nil
}

func SortEdges(edges []*Edge) {
	sort.Slice(edges, func(i, j int) bool {
		return (edges[i].Weight < edges[j].Weight) ||
			(edges[i].Weight == edges[j].Weight && edges[i].Src < edges[j].Src) ||
			(edges[i].Weight == edges[j].Weight && edges[i].Src == edges[j].Src && edges[i].Dest < edges[j].Dest)
	})
}

func WriteGraph(fileName string, edges []*Edge) error {
	SortEdges(edges)

	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	totalWeight := 0
	writer := bufio.NewWriter(file)
	for _, edge := range edges {
		totalWeight += edge.Weight
		_, err := fmt.Fprintf(writer, "%d %d %d\n", edge.Src, edge.Dest, edge.Weight)
		if err != nil {
			return err
		}
	}

	return writer.Flush()
}
