package filestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
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
	session   *session.Session
	config    *S3FSConfig
	delimiter string
	maxKeys   int64
}

func (s3fs *S3FS) GetConfig() *S3FSConfig {
	return s3fs.config
}

func (s3fs *S3FS) ResourceName() string {
	return s3fs.config.S3Bucket
}

func (s3fs *S3FS) GetDir(path PathConfig) (*[]FileStoreResultObject, error) {
	s3Path := strings.TrimPrefix(path.Path, "/")
	s3client := s3.New(s3fs.session)
	params := &s3.ListObjectsV2Input{
		Bucket:            aws.String(s3fs.config.S3Bucket),
		Prefix:            &s3Path,
		Delimiter:         &s3fs.delimiter,
		MaxKeys:           &s3fs.maxKeys,
		ContinuationToken: nil,
	}

	resp, err := s3client.ListObjectsV2(params)
	if err != nil {
		log.Printf("failed to list objects in the bucket - %v", err)
	}

	result := []FileStoreResultObject{}
	var count int = 0
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

	return &result, nil
}

func (s3fs *S3FS) GetObject(path PathConfig) (io.ReadCloser, error) {
	s3Path := strings.TrimPrefix(path.Path, "/")
	svc := s3.New(s3fs.session)
	input := &s3.GetObjectInput{
		Bucket: &s3fs.config.S3Bucket,
		Key:    &s3Path,
	}
	output, err := svc.GetObject(input)
	return output.Body, err
}

func (s3fs *S3FS) DeleteObject(path string) error {
	s3Path := strings.TrimPrefix(path, "/")
	svc := s3.New(s3fs.session)
	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(s3fs.config.S3Bucket),
		Delete: &s3.Delete{
			Objects: []*s3.ObjectIdentifier{
				{
					Key: aws.String(s3Path),
				},
			},
			Quiet: aws.Bool(false),
		},
	}
	output, err := s3fs.deleteObjectsImpl(svc, input)
	log.Println("--------DELETE OPERATION OUTPUT------------")
	log.Print(output)
	log.Println("--------DELETE OPERATION OUTPUT------------")
	return err
}

/*
iter := s3manager.NewDeleteListIterator(svc, &s3.ListObjectsInput{
		Bucket: aws.String(s3fs.config.S3Bucket),
		Prefix: aws.String(s3Path),
	})

	err := s3manager.NewBatchDeleteWithClient(svc).Delete(context.Background(), iter)
*/

func (s3fs *S3FS) Upload(reader io.Reader, key string) error {
	uploader := s3manager.NewUploader(s3fs.session)

	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s3fs.config.S3Bucket),
		Key:    aws.String(key),
		Body:   reader,
	})
	return err
}

func (s3fs *S3FS) UploadFile(filepath string, key string) error {
	file, err := os.Open(filepath)
	if err != nil {
		return errors.New(fmt.Sprintf("Unable to open file %q, %v", filepath, err))
	}

	defer file.Close()
	return s3fs.Upload(file, key)
}

func (s3fs *S3FS) PutObject(path PathConfig, data []byte) (*FileOperationOutput, error) {
	s3Path := strings.TrimPrefix(path.Path, "/")
	svc := s3.New(s3fs.session)
	reader := bytes.NewReader(data)
	input := &s3.PutObjectInput{
		Bucket:        &s3fs.config.S3Bucket,
		Body:          reader,
		ContentLength: aws.Int64(int64(len(data))),
		Key:           &s3Path,
	}
	s3output, err := svc.PutObject(input)
	if err != nil {
		return nil, err
	}
	fmt.Print(s3output)
	output := &FileOperationOutput{
		Md5: *s3output.ETag,
	}
	return output, err
}

func (s3fs *S3FS) CopyObject(sourcePath PathConfig, destPath PathConfig) error {
	source := strings.TrimPrefix(sourcePath.Path, "/")
	dest := strings.TrimPrefix(destPath.Path, "/")
	svc := s3.New(s3fs.session)
	input := s3.CopyObjectInput{
		Bucket:     &s3fs.config.S3Bucket,
		CopySource: &source,
		Key:        &dest,
	}
	_, err := svc.CopyObject(&input)
	return err
}

func (s3fs *S3FS) deleteObjectsImpl(svc *s3.S3, input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
	result, err := svc.DeleteObjects(input)
	return result, err
}

func (s3fs *S3FS) deleteListImpl(svc *s3.S3, input *s3.DeleteObjectsInput) []error {
	errs := []error{}
	for _, obj := range input.Delete.Objects {
		iter := s3manager.NewDeleteListIterator(svc, &s3.ListObjectsInput{
			Bucket: input.Bucket,
			Prefix: obj.Key,
		})

		err := s3manager.NewBatchDeleteWithClient(svc).Delete(context.Background(), iter)
		errs = append(errs, err)
	}
	return errs
}

func (s3fs *S3FS) DeleteObjects(path PathConfig) error {
	svc := s3.New(s3fs.session)
	objects := make([]*s3.ObjectIdentifier, 0, len(path.Paths))
	for _, p := range path.Paths {
		p := p
		s3Path := strings.TrimPrefix(p, "/")
		object := &s3.ObjectIdentifier{
			Key: aws.String(s3Path),
		}
		objects = append(objects, object)
	}

	input := &s3.DeleteObjectsInput{
		Bucket: &s3fs.config.S3Bucket,
		Delete: &s3.Delete{
			Objects: objects,
			Quiet:   aws.Bool(false),
		},
	}

	//output, err := s3fs.deleteObjectsImpl(svc, input)
	errs := s3fs.deleteListImpl(svc, input)
	log.Println("--------DELETE OPERATION OUTPUT------------")
	log.Print(errs)
	log.Println("--------DELETE OPERATION OUTPUT------------")
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func (s3fs *S3FS) InitializeObjectUpload(u UploadConfig) (UploadResult, error) {
	output := UploadResult{}
	svc := s3.New(s3fs.session)
	s3path := u.ObjectPath //@TODO incomoplete
	s3path = strings.TrimPrefix(s3path, "/")
	input := &s3.CreateMultipartUploadInput{
		Bucket: &s3fs.config.S3Bucket,
		Key:    &s3path,
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
		Bucket:        &s3fs.config.S3Bucket,
		Key:           &s3path,
		PartNumber:    aws.Int64(partNumber),
		UploadId:      &u.UploadId,
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
	for i, cuId := range u.ChunkUploadIds {
		cp = append(cp, &s3.CompletedPart{
			ETag:       aws.String(cuId),
			PartNumber: aws.Int64(int64(i + 1)),
		})
	}
	input := &s3.CompleteMultipartUploadInput{
		Bucket:   &s3fs.config.S3Bucket,
		Key:      &s3path,
		UploadId: &u.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: cp,
		},
	}
	result, err := svc.CompleteMultipartUpload(input)
	fmt.Print(result)
	return err
}

func (s3fs *S3FS) Walk(path string, vistorFunction FileVisitFunction) error {
	s3Path := strings.TrimPrefix(path, "/")
	s3delim := ""
	query := &s3.ListObjectsV2Input{
		Bucket:    aws.String(s3fs.config.S3Bucket),
		Prefix:    &s3Path,
		Delimiter: &s3delim,
	}
	svc := s3.New(s3fs.session)

	truncatedListing := true

	for truncatedListing {
		resp, err := svc.ListObjectsV2(query)
		if err != nil {
			return err
		}
		for _, content := range resp.Contents {
			//fmt.Printf("Processing: %s\n", *content.Key)
			fileInfo := &S3FileInfo{content}
			err = vistorFunction("/"+*content.Key, fileInfo)
			if err != nil {
				log.Printf("Visitor Function error: %s\n", err)
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
func (s3fs *S3FS) GetPresignedUrl(path PathConfig, days int) (string, error) {
	s3Path := strings.TrimPrefix(path.Path, "/")
	svc := s3.New(s3fs.session)
	input := &s3.GetObjectInput{
		Bucket: &s3fs.config.S3Bucket,
		Key:    &s3Path,
	}
	req, _ := svc.GetObjectRequest(input)
	urlStr, err := req.Presign(time.Duration(24*days) * time.Hour)
	if err != nil {
		log.Println("Failed to sign request", err)
	}
	return urlStr, err
}

func (s3fs *S3FS) SetObjectPublic(path PathConfig) (string, error) {
	s3Path := strings.TrimPrefix(path.Path, "/")
	svc := s3.New(s3fs.session)
	acl := "public-read"
	input := &s3.PutObjectAclInput{
		Bucket: &s3fs.config.S3Bucket,
		Key:    &s3Path,
		ACL:    &acl,
	}
	aclResp, err := svc.PutObjectAcl(input)
	if err != nil {
		log.Printf("Failed to add public-read ACL on %s\n", s3Path)
		log.Println(aclResp)
	}
	url := fmt.Sprintf("https://%s.s3.amazonaws.com/%s", s3fs.config.S3Bucket, s3Path)
	log.Println(url)
	return url, err
}
