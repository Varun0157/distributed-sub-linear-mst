package utils

import (
	"fmt"
	"time"
)

func Partition[T any](data []T, numPartitions int) ([][]T, error) {
	if numPartitions < 1 {
		return nil, fmt.Errorf("number of partitions must be at least 1")
	}
	if numPartitions > len(data) {
		return nil, fmt.Errorf("number of partitions cannot exceed length of slice")
	}

	result := make([][]T, numPartitions)

	size := len(data) / numPartitions
	extra := len(data) % numPartitions

	start := 0
	for i := 0; i < numPartitions; i++ {
		end := start + size
		if extra > 0 {
			end += 1
			extra -= 1
		}
		result[i] = data[start:end]
		start = end
	}

	return result, nil
}

func RpcTimeout() time.Duration {
	return 120 * time.Second
}
