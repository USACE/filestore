package filestore

import (
	"crypto/md5"
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

type FileStore interface {
	GetDir(string, bool) (*[]FileStoreResultObject, error)
	GetObject(string) (io.ReadCloser, error)
	PutObject(string, []byte) (*FileOperationOutput, error)
	DeleteObjects(path ...string) error
	//PutMultipartObject(u UploadConfig) (UploadResult, error)
	//InitializeMultipartWrite
	//PutPart(u UploadConfig) (UploadResult, error)
	Walk(string, FileVisitFunction) error

	/////depricate
	InitializeObjectUpload(UploadConfig) (UploadResult, error)
	WriteChunk(UploadConfig) (UploadResult, error)
	CompleteObjectUpload(CompletedObjectUploadConfig) error
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
		if s3config.Mock {
			cfg.WithDisableSSL(s3config.S3DisableSSL)
			cfg.WithS3ForcePathStyle(s3config.S3ForcePathStyle)
			if s3config.S3Endpoint != "" {
				cfg.WithEndpoint(s3config.S3Endpoint)
			}
		}
		sess, err := session.NewSession(cfg)
		if err != nil {
			return nil, err
		}

		fs := S3FS{
			session: sess,
			config:  &s3config,
			maxKeys: 1000,
		}
		return &fs, nil

	default:
		return nil, fmt.Errorf("Invalid File System System Type Configuration: %v", scType)
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

//@TODO should be able to depricate this function
/*
func buildUrl(urlparts ...string) string {
	var b strings.Builder
	t := "%s"
	for _, p := range urlparts {
		fmt.Println(p)
		if !strings.HasPrefix(p, "/") {
			t = "/%s"
		}
		if strings.HasSuffix(p, "/") {
			p = p[:len(p)-1]
		}
		fmt.Fprintf(&b, t, p)
	}
	return b.String()
}
*/
