# filestore

## Overview

`filestore` is a generic implementation for working with local files or cloud-based objects through a single interface. Currently, `filestore` supports AWS S3, with intentions of supporting Azure Blob Storage in the future.



## Installation

```sh
go get github.com/USACE/filestore
```


## Usage

```go
package main

import "github.com/USACE/filestore"

func main() {

	config := filestore.S3FSConfig{
		S3Id:     "************",  // AWS_ACCESS_KEY_ID
		S3Key:    "************",  // AWS_SECRET_ACCESS_KEY
		S3Region: "us-east-1",
		S3Bucket: "my-bucket",
	}

	fs, err := filestore.NewFileStore(config)
	if err != nil {
		log.Fatal(err)
	}

	err = fs.Walk("path-prefix", walkFunc)
	if err != nil {
		log.Fatal(err)
	}

}

func walkFunc(path string, file os.FileInfo) error {
	fmt.Println(path)
	return nil
}
```