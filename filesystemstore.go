package filestore

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/google/uuid"
)

//@TODO this is kind of clunky.  BlockFSConfig is only used in NewFileStore as a type case so we know to create a Block File Store
//as of now I don't actually need any config properties
type BlockFSConfig struct{}

type BlockFS struct{}

func (b *BlockFS) GetDir(path string, recursive bool) (*[]FileStoreResultObject, error) {
	fmt.Println(path)

	var objects []FileStoreResultObject
	switch recursive {
	case true:
		objects = make([]FileStoreResultObject, 0)
		i := 0
		err := filepath.Walk(
			path,
			func(path string, file os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				objects = append(objects, FileStoreResultObject{
					ID:         i,
					Name:       file.Name(),
					Size:       strconv.FormatInt(file.Size(), 10),
					Path:       filepath.Dir(path),
					Type:       filepath.Ext(file.Name()),
					IsDir:      file.IsDir(),
					Modified:   file.ModTime(),
					ModifiedBy: "",
				})
				i++
				return nil
			})
		if err != nil {
			return nil, err
		}

	case false:
		contents, err := ioutil.ReadDir(path)
		if err != nil {
			return nil, err
		}
		objects = make([]FileStoreResultObject, len(contents))
		for i, f := range contents {
			objects[i] = FileStoreResultObject{
				ID:         i,
				Name:       f.Name(),
				Size:       strconv.FormatInt(f.Size(), 10),
				Path:       path,
				Type:       filepath.Ext(f.Name()),
				IsDir:      f.IsDir(),
				Modified:   f.ModTime(),
				ModifiedBy: "",
			}
		}
	}
	return &objects, nil
}

func (b *BlockFS) GetObject(path string) (io.ReadCloser, error) {
	return os.Open(path)
}

func (b *BlockFS) DeleteObjects(path ...string) error {
	var err error
	for _, p := range path {
		if isDir(p) {
			err = os.RemoveAll(p)
		} else {
			err = os.Remove(p)
		}
	}
	return err
}

func (b *BlockFS) PutObject(path string, data []byte) (*FileOperationOutput, error) {
	if len(data) == 0 {
		f := FileOperationOutput{}
		err := os.MkdirAll(filepath.Dir(path), os.ModePerm)
		return &f, err
	} else {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		_, err = f.Write(data)
		md5 := getFileMd5(f)
		output := &FileOperationOutput{
			Md5: md5,
		}
		return output, err
	}
}

func (b *BlockFS) InitializeObjectUpload(u UploadConfig) (UploadResult, error) {
	fmt.Println(u.ObjectPath)
	result := UploadResult{}
	os.MkdirAll(filepath.Dir(u.ObjectPath), os.ModePerm) //@TODO incomplete
	f, err := os.Create(u.ObjectPath)                    //@TODO incomplete
	if err != nil {
		return result, err
	}
	_ = f.Close()
	result.ID = uuid.New().String()
	return result, nil
}

func (b *BlockFS) WriteChunk(u UploadConfig) (UploadResult, error) {
	result := UploadResult{}
	//var err error
	mutex := &sync.Mutex{}
	mutex.Lock()
	defer mutex.Unlock()
	f, err := os.OpenFile(u.ObjectPath, os.O_WRONLY|os.O_CREATE, 0644) //@TODO incomplete
	if err != nil {
		return result, err
	}
	defer f.Close()
	_, err = f.WriteAt(u.Data, (u.ChunkId * chunkSize))
	result.WriteSize = len(u.Data)
	return result, err
}

func (b *BlockFS) CompleteObjectUpload(u CompletedObjectUploadConfig) error {
	//return md5 hash for file
	return nil
}

func (b *BlockFS) Walk(path string, vistorFunction FileVisitFunction) error {
	err := filepath.Walk(path,
		func(path string, fileinfo os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			err = vistorFunction(path, fileinfo)
			return err
		})
	return err
}
