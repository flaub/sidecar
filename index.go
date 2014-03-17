package main

import (
	"sort"
)

type Index map[string]string

func NewIndex() Index {
	return make(Index)
}

func (this Index) SortedKeys() []string {
	keys := make([]string, 0, len(this))
	for key := range this {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
