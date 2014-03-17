package main

import (
	"crypto/md5"
	"fmt"
	"github.com/docopt/docopt.go"
	"hash"
	"io"
	"os"
	"reflect"
	"runtime"
	"strings"
)

var USAGE = `
Usage:
  sidecar init --url=<url>
  sidecar ls
  sidecar add <path>...
  sidecar release <path>...
  sidecar rm <path>...
  sidecar mv <from>... <to>
  sidecar cp <from>... <to>
  sidecar status
  sidecar push
  sidecar pull

options:
   -h, --help       Show this screen.
   -u, --url=<url>  Specify archive URL.
`

const (
	k_manifest = "sidecar.json"
	k_store    = ".sidecar"
)

func ComputeHash(hasher hash.Hash, path string) (*Hash, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	_, err = io.Copy(hasher, file)
	if err != nil {
		return nil, err
	}
	return &Hash{hasher.Sum(nil)}, nil
}

type Arguments map[string]interface{}

type CommandProc struct {
	args Arguments
}

func NewCommandProc(args Arguments) *CommandProc {
	return &CommandProc{args}
}

func (this *CommandProc) dispatch() {
	typ := reflect.TypeOf(this)
	for key, value := range this.args {
		name := fmt.Sprintf("Do%s", strings.Title(key))
		method, ok := typ.MethodByName(name)
		if ok && value.(bool) {
			method.Func.Call([]reflect.Value{reflect.ValueOf(this)})
			return
		}
	}
}

func (this *CommandProc) DoInit() {
	url := this.args["--url"].(string)
	manifest := NewManifest(url)
	err := manifest.Load()
	if err == nil {
		fmt.Println("sidecar is already initialized.")
		return
	}
	os.Mkdir(k_store, 0775)
	err = manifest.Save()
	if err != nil {
		fmt.Println("Problem saving %q: %v", k_manifest, err)
		os.Exit(1)
	}
}

func (this *CommandProc) DoLs() {
	manifest := LoadManifest()
	for _, key := range manifest.Index.SortedKeys() {
		fmt.Println(key)
	}
}

type StatusWork struct {
	file   *File
	result string
}

func (this *CommandProc) DoStatus() {
	manifest := LoadManifest()

	url := manifest.Settings["url"]
	archive, err := NewArchive(url)
	if err != nil {
		fmt.Printf("Problem accessing archive %q: %s\n", url, err)
		os.Exit(1)
	}

	queue := NewParallelOrderedQueue(10, func(item interface{}) interface{} {
		work := item.(*StatusWork)
		ok, err := archive.Exists(work.file)
		if err != nil {
			work.result = err.Error()
		} else if ok {
			work.result = "ok"
		} else {
			work.result = "not pushed"
		}
		return work
	})

	go func() {
		for _, path := range manifest.Index.SortedKeys() {
			file := NewFile(path, manifest.Index[path])
			queue.Add(&StatusWork{file, ""})
		}
		queue.End()
	}()

	for queue.Next() {
		work := queue.Current().(*StatusWork)
		fmt.Printf("%q: %v\n", work.file.path, work.result)
	}
}

type FileWork struct {
	file *File
	err  error
}

func (this *CommandProc) DoPush() {
	manifest := LoadManifest()

	url := manifest.Settings["url"]
	archive, err := NewArchive(url)
	if err != nil {
		fmt.Printf("Problem accessing archive %q: %s\n", url, err)
		os.Exit(1)
	}

	queue := NewParallelOrderedQueue(10, func(item interface{}) interface{} {
		work := item.(*FileWork)
		fmt.Printf("Pushing: %q\n", work.file.path)
		work.err = archive.Push(work.file)
		return work
	})

	go func() {
		for _, path := range manifest.Index.SortedKeys() {
			file := NewFile(path, manifest.Index[path])
			queue.Add(&FileWork{file, nil})
		}
		queue.End()
	}()

	for queue.Next() {
		work := queue.Current().(*FileWork)
		if work.err != nil {
			fmt.Printf("Problem pushing %q: %s\n", work.file.path, work.err)
		} else {
			fmt.Printf("Pushed %q\n", work.file.path)
		}
	}
}

func (this *CommandProc) DoPull() {
	manifest := LoadManifest()

	url := manifest.Settings["url"]
	archive, err := NewArchive(url)
	if err != nil {
		fmt.Printf("Problem accessing archive %q: %s\n", url, err)
		os.Exit(1)
	}

	queue := NewParallelOrderedQueue(10, func(item interface{}) interface{} {
		work := item.(*FileWork)
		if !work.file.Verify() {
			fmt.Printf("Pulling: %s\n", work.file.path)
			work.err = archive.Pull(work.file)
			if work.err != nil {
				return work
			}
		}
		work.err = work.file.MkLink()
		return work
	})

	go func() {
		for _, path := range manifest.Index.SortedKeys() {
			file := NewFile(path, manifest.Index[path])
			queue.Add(&FileWork{file, nil})
		}
		queue.End()
	}()

	for queue.Next() {
		work := queue.Current().(*FileWork)
		if work.err != nil {
			fmt.Printf("Problem pulling %q: %s\n", work.file.path, work.err)
		} else {
			fmt.Printf("Pulled %q\n", work.file.path)
		}
	}
}

func (this *CommandProc) DoAdd() {
	manifest := LoadManifest()

	for _, path := range this.args["<path>"].([]string) {
		fi, err := os.Lstat(path)
		if err != nil {
			fmt.Printf("%s\n", err)
			continue
		}
		if err == nil && fi.Mode()&os.ModeSymlink != 0 {
			if _, ok := manifest.Index[path]; ok {
				fmt.Printf("Already in the store, ignoring: %q\n", path)
			} else {
				fmt.Printf("Path is a symbolic link, ignoring: %q\n", path)
			}
			continue
		}

		hash, err := ComputeHash(md5.New(), path)
		if err != nil {
			fmt.Printf("Could not compute SHA256 hash: %q\n", path)
			continue
		}

		manifest.Index[path] = hash.EncodeHex()

		file := NewFile(path, manifest.Index[path])
		err = file.Add()
		if err != nil {
			fmt.Printf("Problem adding %q: %s\n", path, err)
		} else {
			fmt.Printf("Adding: %q -> %q\n", path, file.relativePath())
		}
	}

	err := manifest.Save()
	if err != nil {
		fmt.Printf("Problem saving %q: %s\n", k_manifest, err)
		os.Exit(1)
	}
}

func (this *CommandProc) DoRelease() {
}

func (this *CommandProc) DoRm() {
}

func (this *CommandProc) DoCp() {
}

func (this *CommandProc) DoMv() {
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	args, err := docopt.Parse(USAGE, nil, true, "", false)
	if err != nil {
		return
	}

	cmdproc := NewCommandProc(args)
	cmdproc.dispatch()
}
