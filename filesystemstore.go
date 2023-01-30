package filestore

import (
	"errors"
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

func (b *BlockFS) GetDir(path PathConfig) (*[]FileStoreResultObject, error) {
	fmt.Println(path.Path)
	dirContents, err := ioutil.ReadDir(path.Path)
	if err != nil {
		return nil, err
	}
	objects := make([]FileStoreResultObject, len(dirContents))
	for i, f := range dirContents {
		size := strconv.FormatInt(f.Size(), 10)
		objects[i] = FileStoreResultObject{
			ID:         i,
			Name:       f.Name(),
			Size:       size,
			Path:       path.Path,
			Type:       filepath.Ext(f.Name()),
			IsDir:      f.IsDir(),
			Modified:   f.ModTime(),
			ModifiedBy: "",
		}
	}
	return &objects, nil
}

func (b *BlockFS) ResourceName() string {
	return ""
}

func (b *BlockFS) GetObject(path PathConfig) (io.ReadCloser, error) {
	return os.Open(path.Path)
}

func (b *BlockFS) DeleteObject(path string) error {
	var err error
	if isDir(path) {
		err = os.RemoveAll(path)
	} else {
		err = os.Remove(path)
	}
	return err
}

func (b *BlockFS) PutObject(path PathConfig, data []byte) (*FileOperationOutput, error) {
	if len(data) == 0 {
		f := FileOperationOutput{}
		err := os.MkdirAll(filepath.Dir(path.Path), os.ModePerm)
		return &f, err
	} else {
		f, err := os.OpenFile(path.Path, os.O_WRONLY|os.O_CREATE, 0644)
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

func (b *BlockFS) CopyObject(sourcePath PathConfig, destPath PathConfig) error {
	src, err := os.Open(sourcePath.Path)
	if err != nil {
		return err
	}
	defer src.Close()

	dest, err := os.Create(destPath.Path)
	if err != nil {
		return err
	}
	defer dest.Close()

	_, err = io.Copy(src, dest)
	return err
}

func (b *BlockFS) DeleteObjects(path PathConfig) error {
	var err error
	for _, p := range path.Paths {
		if isDir(p) {
			err = os.RemoveAll(p)
		} else {
			err = os.Remove(p)
		}
	}
	return err
}

func (b *BlockFS) UploadFile(filepath string, key string) error {
	return errors.New("Not Implemented")
}

func (b *BlockFS) Upload(reader io.Reader, key string) error {
	return errors.New("Not Implemented")
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
