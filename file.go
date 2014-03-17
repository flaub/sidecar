package main

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

type Hash struct {
	buf []byte
}

func (this *Hash) EncodeHex() string {
	return hex.EncodeToString(this.buf)
}

func (this *Hash) EncodeBase64() string {
	return base64.StdEncoding.EncodeToString(this.buf)
}

type File struct {
	path string
	hash Hash
}

func NewFile(path, hash string) *File {
	digest, err := hex.DecodeString(hash)
	if err != nil {
		panic(err)
	}
	return &File{path, Hash{digest}}
}

func (this *File) storagePath() string {
	return filepath.Join(k_store, this.hash.EncodeHex())
}

func (this *File) relativePath() string {
	path, err := filepath.Rel(filepath.Dir(this.path), this.storagePath())
	if err != nil {
		panic(err)
	}
	return path
}

func (this *File) Add() error {
	err := os.Rename(this.path, this.storagePath())
	if err != nil {
		return err
	}

	err = this.Symlink()
	if err != nil {
		os.Rename(this.storagePath(), this.path)
		return err
	}

	return nil
}

func (this *File) Symlink() error {
	return os.Symlink(this.relativePath(), this.path)
}

func (this *File) Verify() bool {
	hash, err := ComputeHash(md5.New(), this.path)
	if err != nil {
		return false
	}
	return hash.EncodeBase64() == this.hash.EncodeBase64()
}

func (this *File) MkLink() error {
	// possible states:
	// - path does not exist
	//   create new symlink
	// - good symlink exists
	//   continue
	// - bad symlink exists
	//   repair symlink

	fi, err := os.Lstat(this.path)
	if err == nil {
		if fi.Mode()&os.ModeSymlink != 0 {
			link, err := os.Readlink(this.path)
			if err == nil && link == this.relativePath() {
				return nil
			}
		}

		err = os.Remove(this.path)
		if err != nil {
			return err
		}
	}

	fmt.Printf("Linking: %q -> %q\n", this.path, this.relativePath())
	return this.Symlink()
}
