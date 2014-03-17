package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
)

type Index map[string]string

func NewIndex() Index {
	return make(Index)
}

func LoadIndex() Index {
	index := NewIndex()
	err := index.Load()
	if err != nil {
		fmt.Printf("Could not load index: %q\n", k_index)
		fmt.Println("Use \"sidecar init\" if you have not yet done so.")
		os.Exit(1)
	}
	return index
}

func (this Index) Load() error {
	file, err := os.Open(k_index)
	if err != nil {
		return err
	}
	defer file.Close()
	return json.NewDecoder(file).Decode(&this)
}

func (this Index) Save() error {
	data, err := json.MarshalIndent(&this, "", "    ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(k_index, data, 0655)
}

func (this Index) SortedKeys() []string {
	keys := make([]string, 0, len(this))
	for key := range this {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
