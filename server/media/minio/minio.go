package minio

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/tinode/chat/server/media"
	"github.com/tinode/chat/server/store"
	"github.com/tinode/chat/server/store/types"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const(
	handlerName = "minio"
)

type minioConfig struct {
	AccessKeyId     string  `json:"access_key_id"`
	SecretAccessKey string 	`json:"secret_access_key"`
	Region          string 	`json:"region"`
	BucketName      string 	`json:"bucket"`
	Endpoint        string 	`json:"endpoint"`
	UseSSL          bool	`json:"useSSL"`
	ServeURL 		string  `json:"serve_url"`
}

type minioHandler struct {
	svc  *minio.Client
	conf minioConfig
}

func (minioH *minioHandler) Init(jsonconf string) error {
	var err error
	if err = json.Unmarshal([]byte(jsonconf), &minioH.conf); err != nil {
		return errors.New("failed to parse config: " + err.Error())
	}
	println(minioH.conf.ServeURL)
	println(minioH.conf.UseSSL)
	println(minioH.conf.SecretAccessKey)
	println(minioH.conf.AccessKeyId)
	println(minioH.conf.Region)
	println(minioH.conf.BucketName)
	println(minioH.conf.Endpoint)

	// Initialize minio client object.
	var minioClient *minio.Client
	minioClient, err = minio.New(minioH.conf.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(minioH.conf.AccessKeyId, minioH.conf.SecretAccessKey, ""),
		Secure: minioH.conf.UseSSL,
	})
	if err != nil {
		return err
	}

	minioH.svc = minioClient

	var found bool
	found, err = minioH.svc.BucketExists(context.Background(), minioH.conf.BucketName)
	if found {
		if err == nil {
			return nil
		}
	} else {
		err = minioH.svc.MakeBucket(context.Background(), minioH.conf.BucketName, minio.MakeBucketOptions{
			Region: minioH.conf.Region,
			ObjectLocking: false,
		})
		if err != nil {
			return err
		}
	}
	return err
}

func (minioH *minioHandler) Redirect(method, url string) (string, error) {
	return "", types.ErrUnsupported
}

func (minioH *minioHandler) Upload(fdef *types.FileDef, file io.ReadSeeker) (string, error) {
	var err error
	key := fdef.Uid().String32()
	fdef.Location = key
	uploadInfo, err := minioH.svc.PutObject(context.Background(), minioH.conf.BucketName, key, file, fdef.Size, minio.PutObjectOptions{ContentType:"application/octet-stream"})
	if err != nil {
		return "", err
	}

	return uploadInfo.Location, nil
}

func (minioH *minioHandler) Download(url string) (*types.FileDef, media.ReadSeekCloser, error) {
	return nil, nil, types.ErrUnsupported
}

func (minioH *minioHandler) Delete(locations []string) error {
	for _, key := range locations {
		object, err := minioH.svc.GetObject(context.Background(), minioH.conf.BucketName, key, minio.GetObjectOptions{})
		if err != nil {
			return err
		}
		sObject, err := object.Stat()
		if err != nil {
			return err
		}
		minioH.svc.RemoveObject(context.Background(), minioH.conf.BucketName, sObject.Key, minio.RemoveObjectOptions{})
	}

	return nil
}

func (minioH *minioHandler) GetIdFromUrl(url string) types.Uid {
	return media.GetIdFromUrl(url, minioH.conf.ServeURL)
}

// getFileRecord given file ID reads file record from the database.
func (minioH *minioHandler) getFileRecord(fid types.Uid) (*types.FileDef, error) {
	fd, err := store.Files.Get(fid.String())
	if err != nil {
		return nil, err
	}
	if fd == nil {
		return nil, types.ErrNotFound
	}
	return fd, nil
}

func init() {
	store.RegisterMediaHandler(handlerName, &minioHandler{})
}