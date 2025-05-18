package main

import (
	"fmt"
	"hash/fnv"
)

type FragColour int

const (
	RedFrag FragColour = iota
	BlueFrag
)

type SharedRandomness struct {
	globalSeed int
}

func NewSharedRandomness() *SharedRandomness {
	globalSeed := 42
	return &SharedRandomness{
		globalSeed: globalSeed,
	}
}

func (sr *SharedRandomness) GetFragmentColour(round, id int) FragColour {
	inputStr := fmt.Sprintf("%d-%d-%d", sr.globalSeed, round, id)

	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(inputStr))
	hashValue := hasher.Sum32()

	if hashValue%2 == 0 {
		return BlueFrag
	}
	return RedFrag
}
