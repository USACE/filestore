package filestore

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3FileInfo struct {
	s3 *s3.Object
}

func (obj *S3FileInfo) Name() string {
	return *obj.s3.Key
}

func (obj *S3FileInfo) Size() int64 {
	return *obj.s3.Size
}

func (obj *S3FileInfo) Mode() os.FileMode {
	return os.ModeIrregular
}

func (obj *S3FileInfo) ModTime() time.Time {
	return *obj.s3.LastModified
}

func (obj *S3FileInfo) IsDir() bool {
	return false
}

func (obj *S3FileInfo) Sys() interface{} {
	return nil
}

type S3FSConfig struct {
	S3Id     string
	S3Key    string
	S3Region string
	S3Bucket string
}

type S3FS struct {
	session *session.Session
	config  *S3FSConfig
	maxKeys int64
}

func (s3fs *S3FS) GetDir(path string, recursive bool) (*[]FileStoreResultObject, error) {
	s3Path := strings.Trim(path, "/") + "/"
	var delim string
	if !recursive {
		delim = "/"
	}
	s3client := s3.New(s3fs.session)
	query := &s3.ListObjectsV2Input{
		Bucket:    aws.String(s3fs.config.S3Bucket),
		Prefix:    aws.String(s3Path),
		Delimiter: aws.String(delim),
		MaxKeys:   aws.Int64(s3fs.maxKeys),
	}

	result := []FileStoreResultObject{}
	truncatedListing := true
	var count int
	for truncatedListing {

		resp, err := s3client.ListObjectsV2(query)
		if err != nil {
			return nil, err
		}

		for _, cp := range resp.CommonPrefixes {
			w := FileStoreResultObject{
				ID:         count,
				Name:       filepath.Base(*cp.Prefix),
				Size:       "",
				Path:       *cp.Prefix,
				Type:       "",
				IsDir:      true,
				ModifiedBy: "",
			}
			count++
			result = append(result, w)
		}

		for _, object := range resp.Contents {
			w := FileStoreResultObject{
				ID:         count,
				Name:       filepath.Base(*object.Key),
				Size:       strconv.FormatInt(*object.Size, 10),
				Path:       filepath.Dir(*object.Key),
				Type:       filepath.Ext(*object.Key),
				IsDir:      false,
				Modified:   *object.LastModified,
				ModifiedBy: "",
			}
			count++
			result = append(result, w)
		}

		query.ContinuationToken = resp.NextContinuationToken
		truncatedListing = *resp.IsTruncated
	}

	return &result, nil
}

func (s3fs *S3FS) GetObject(path string) (io.ReadCloser, error) {
	s3Path := strings.TrimPrefix(path, "/")
	svc := s3.New(s3fs.session)
	input := &s3.GetObjectInput{
		Bucket: aws.String(s3fs.config.S3Bucket),
		Key:    aws.String(s3Path),
	}
	output, err := svc.GetObject(input)
	return output.Body, err
}

func (s3fs *S3FS) PutObject(path string, data []byte) (*FileOperationOutput, error) {
	s3Path := strings.TrimPrefix(path, "/")
	svc := s3.New(s3fs.session)
	reader := bytes.NewReader(data)
	input := &s3.PutObjectInput{
		Body:          reader,
		ContentLength: aws.Int64(int64(len(data))),
		Key:           aws.String(s3Path),
	}
	s3output, err := svc.PutObject(input)
	if err != nil {
		return nil, err
	}
	return &FileOperationOutput{Md5: *s3output.ETag}, nil
}

func (s3fs *S3FS) DeleteObjects(path ...string) error {
	svc := s3.New(s3fs.session)
	objects := make([]*s3.ObjectIdentifier, 0, len(path))
	for _, p := range path {
		s3Path := strings.TrimPrefix(p, "/")
		object := &s3.ObjectIdentifier{
			Key: aws.String(s3Path),
		}
		objects = append(objects, object)
	}

	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(s3fs.config.S3Bucket),
		Delete: &s3.Delete{
			Objects: objects,
			Quiet:   aws.Bool(false),
		},
	}

	_, err := svc.DeleteObjects(input)
	return err
}

func (s3fs *S3FS) InitializeObjectUpload(u UploadConfig) (UploadResult, error) {
	output := UploadResult{}
	svc := s3.New(s3fs.session)
	s3path := u.ObjectPath //@TODO incomplete
	s3path = strings.TrimPrefix(s3path, "/")
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s3fs.config.S3Bucket),
		Key:    aws.String(s3path),
	}

	resp, err := svc.CreateMultipartUpload(input)
	if err != nil {
		return output, err
	}
	output.ID = *resp.UploadId
	return output, nil
}

func (s3fs *S3FS) WriteChunk(u UploadConfig) (UploadResult, error) {
	s3path := u.ObjectPath //@TODO incomplete
	s3path = strings.TrimPrefix(s3path, "/")
	svc := s3.New(s3fs.session)
	partNumber := u.ChunkId + 1 //aws chunks are 1 to n, our chunks are 0 referenced
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(u.Data),
		Bucket:        aws.String(s3fs.config.S3Bucket),
		Key:           aws.String(s3path),
		PartNumber:    aws.Int64(partNumber),
		UploadId:      aws.String(u.UploadId),
		ContentLength: aws.Int64(int64(len(u.Data))),
	}
	result, err := svc.UploadPart(partInput)

	if err != nil {
		return UploadResult{}, err
	}
	output := UploadResult{
		WriteSize: len(u.Data),
		ID:        *result.ETag,
	}
	return output, nil
}

func (s3fs *S3FS) CompleteObjectUpload(u CompletedObjectUploadConfig) error {
	s3path := u.ObjectPath //@TODO incomplete
	s3path = strings.TrimPrefix(s3path, "/")
	svc := s3.New(s3fs.session)
	cp := []*s3.CompletedPart{}
	for i, cuID := range u.ChunkUploadIds {
		cp = append(cp, &s3.CompletedPart{
			ETag:       aws.String(cuID),
			PartNumber: aws.Int64(int64(i + 1)),
		})
	}
	input := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(s3fs.config.S3Bucket),
		Key:      aws.String(s3path),
		UploadId: aws.String(u.UploadId),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: cp,
		},
	}
	_, err := svc.CompleteMultipartUpload(input)
	return err
}

func (s3fs *S3FS) Walk(path string, vistorFunction FileVisitFunction) error {
	s3Path := strings.TrimPrefix(path, "/")
	s3delim := ""
	query := &s3.ListObjectsV2Input{
		Bucket:    aws.String(s3fs.config.S3Bucket),
		Prefix:    aws.String(s3Path),
		Delimiter: aws.String(s3delim),
	}
	svc := s3.New(s3fs.session)

	truncatedListing := true

	for truncatedListing {
		resp, err := svc.ListObjectsV2(query)
		if err != nil {
			return err
		}
		for _, content := range resp.Contents {
			fileInfo := &S3FileInfo{content}
			err := vistorFunction("/"+*content.Key, fileInfo)
			if err != nil {
				return err
			}
		}
		query.ContinuationToken = resp.NextContinuationToken
		truncatedListing = *resp.IsTruncated
	}
	return nil
}

/*
  these functions are not part of the filestore interface and are unique to the S3FS
*/
func (s3fs *S3FS) SharedAccessURL(path string, expiration time.Duration) (string, error) {
	s3Path := strings.TrimPrefix(path, "/")
	svc := s3.New(s3fs.session)
	input := &s3.GetObjectInput{
		Bucket: aws.String(s3fs.config.S3Bucket),
		Key:    aws.String(s3Path),
	}
	req, _ := svc.GetObjectRequest(input)
	return req.Presign(expiration)
}

func (s3fs *S3FS) SetObjectPublic(path string) (string, error) {
	s3Path := strings.TrimPrefix(path, "/")
	svc := s3.New(s3fs.session)
	acl := "public-read"
	url := fmt.Sprintf("https://%s.s3.amazonaws.com/%s", s3fs.config.S3Bucket, s3Path)
	input := &s3.PutObjectAclInput{
		Bucket: aws.String(s3fs.config.S3Bucket),
		Key:    aws.String(s3Path),
		ACL:    aws.String(acl),
	}
	_, err := svc.PutObjectAcl(input)
	return url, err
}
