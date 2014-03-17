package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type Settings map[string]string

type Manifest struct {
	Settings Settings
	Index    Index
}

func NewManifest(url string) *Manifest {
	settings := make(Settings)
	settings["url"] = url
	return &Manifest{
		Settings: settings,
		Index:    NewIndex(),
	}
}

func LoadManifest() *Manifest {
	manifest := &Manifest{}
	err := manifest.Load()
	if err != nil {
		fmt.Printf("Could not load %q\n", k_manifest)
		fmt.Println("Use \"sidecar init\" if you have not yet done so.")
		os.Exit(1)
	}
	return manifest
}

func (this *Manifest) Load() error {
	file, err := os.Open(k_manifest)
	if err != nil {
		return err
	}
	defer file.Close()
	return json.NewDecoder(file).Decode(this)
}

func (this *Manifest) Save() error {
	data, err := json.MarshalIndent(this, "", "    ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(k_manifest, data, 0655)
}
