package filestore

import (
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

type PATHTYPE int

const (
	FILE PATHTYPE = iota
	FOLDER
)

var chunkSize int64 = 10 * 1024 * 1024

type PathConfig struct {
	Path  string
	Paths []string
}

type FileOperationOutput struct {
	Md5 string
}

type FileStoreResultObject struct {
	ID         int       `json:"id"`
	Name       string    `json:"fileName"`
	Size       string    `json:"size"`
	Path       string    `json:"filePath"`
	Type       string    `json:"type"`
	IsDir      bool      `json:"isdir"`
	Modified   time.Time `json:"modified"`
	ModifiedBy string    `json:"modifiedBy"`
}

type UploadConfig struct {
	//PathInfo   models.ModelPathInfo
	//DirPath    string
	//FilePath   string
	ObjectPath string
	//ObjectName string
	ChunkId int64
	//FileId     uuid.UUID
	UploadId string
	Data     []byte
}

type CompletedObjectUploadConfig struct {
	UploadId string
	//PathInfo       models.ModelPathInfo
	//DirPath        string
	//FilePath       string
	ObjectPath string
	//ObjectName     string
	ChunkUploadIds []string
}

type UploadResult struct {
	ID         string `json:"id"`
	WriteSize  int    `json:"size"`
	IsComplete bool   `json:"isComplete"`
}

type FileVisitFunction func(path string, file os.FileInfo) error

//@TODO evaluate PathConfig as an input.  should this just be a string path.....
type FileStore interface {
	GetDir(path PathConfig) (*[]FileStoreResultObject, error)
	GetObject(PathConfig) (io.ReadCloser, error)
	PutObject(PathConfig, []byte) (*FileOperationOutput, error)
	//Writer(PathConfig) (io.Writer, error)
	CopyObject(source PathConfig, dest PathConfig) error
	ResourceName() string
	Upload(reader io.Reader, key string) error
	UploadFile(filepth string, key string) error
	InitializeObjectUpload(UploadConfig) (UploadResult, error)
	WriteChunk(UploadConfig) (UploadResult, error)
	CompleteObjectUpload(CompletedObjectUploadConfig) error
	DeleteObject(path string) error //depricate eventually?
	DeleteObjects(path PathConfig) error
	Walk(string, FileVisitFunction) error
}

func NewFileStore(config interface{}) (FileStore, error) {
	switch scType := config.(type) {
	case BlockFSConfig:
		fs := BlockFS{}
		return &fs, nil

	case S3FSConfig:
		s3config := config.(S3FSConfig)
		creds := credentials.NewStaticCredentials(s3config.S3Id, s3config.S3Key, "")
		cfg := aws.NewConfig().WithRegion(s3config.S3Region).WithCredentials(creds)
		sess, err := session.NewSession(cfg)
		if err != nil {
			return nil, err
		}

		fs := S3FS{
			session:   sess,
			config:    &s3config,
			delimiter: "/",
			maxKeys:   1000,
		}
		return &fs, nil

	default:
		return nil, errors.New(fmt.Sprintf("Invalid File System System Type Configuration: %v", scType))
	}
}

type PathParts struct {
	Parts []string
}

func (pp PathParts) ToPath(additionalParts ...string) string {
	parts := append(pp.Parts, additionalParts...)
	return buildUrl(parts, FOLDER)
}

func (pp PathParts) ToFilePath(additionalParts ...string) string {
	parts := append(pp.Parts, additionalParts...)
	return buildUrl(parts, FILE)
}

func sanitizePath(path string) string {
	return strings.ReplaceAll(path, "..", "")
}

//@TODO this is duplicated!!!!
func buildUrl(urlparts []string, pathType PATHTYPE) string {
	var b strings.Builder
	t := "/%s"
	for _, p := range urlparts {
		p = strings.Trim(strings.ReplaceAll(p, "//", "/"), "/")
		//p = strings.Trim(p, "/")
		if p != "" {
			fmt.Fprintf(&b, t, p)
		}
	}
	if pathType == FOLDER {
		fmt.Fprintf(&b, "%s", "/")
	}
	return sanitizePath(b.String())
}

func getFileMd5(f *os.File) string {
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func isDir(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return fi.Mode().IsDir()
}
