package main

import (
	"fmt"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"io"
	_url "net/url"
	"os"
)

type Archive struct {
	bucket *s3.Bucket
	root   string
}

func NewArchive(rawurl string) (*Archive, error) {
	url, err := _url.Parse(rawurl)
	if err != nil {
		return nil, err
	}

	if url.Scheme != "s3" {
		fmt.Printf("Only the S3 archive type is supported.\n")
		os.Exit(1)
	}

	regionName := os.Getenv("AWS_DEFAULT_REGION")
	var region aws.Region
	if regionName == "" {
		region = aws.USEast
	} else {
		region = aws.Regions[regionName]
	}

	auth, err := aws.EnvAuth()
	if err != nil {
		return nil, err
	}

	s3c := s3.New(auth, region)

	return &Archive{
		bucket: s3c.Bucket(url.Host),
		root:   url.Path,
	}, nil
}

func (this *Archive) Exists(file *File) (bool, error) {
	path := fmt.Sprintf("%s/%s", this.root, file.hash.EncodeHex())
	// fmt.Printf("Checking s3://%s%s\n", this.bucket.Name, path)
	resp, err := this.bucket.Head(path, nil)
	if err != nil {
		if err.Error() == "404 Not Found" {
			return false, nil
		}
		return false, err
	}
	etag := resp.Header.Get("ETag")
	if etag != fmt.Sprintf("%q", file.hash.EncodeHex()) {
		fmt.Printf("Detected corrupt file in archive, you should push to fix it: %q\n", file.path)
		return false, nil
	}
	return true, nil
}

func (this *Archive) Push(file *File) error {
	ok, err := this.Exists(file)
	if ok && err == nil {
		return nil
	}

	fi, err := os.Stat(file.storagePath())
	if err != nil {
		return err
	}

	reader, err := os.Open(file.storagePath())
	if err != nil {
		return err
	}

	path := fmt.Sprintf("%s/%s", this.root, file.hash.EncodeHex())
	options := s3.Options{
		SSE:        true,
		ContentMD5: file.hash.EncodeBase64(),
	}

	return this.bucket.PutReader(path, reader, fi.Size(), "application/octet-stream",
		s3.BucketOwnerFull, options)
}

func (this *Archive) Pull(file *File) error {
	path := fmt.Sprintf("%s/%s", this.root, file.hash.EncodeHex())
	rc, err := this.bucket.GetReader(path)
	if err != nil {
		return err
	}
	defer rc.Close()
	writer, err := os.Create(file.storagePath())
	if err != nil {
		return err
	}
	defer writer.Close()
	_, err = io.Copy(writer, rc)
	return err
}
