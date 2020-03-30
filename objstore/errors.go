package objstore

import (
	"errors"

	minio "github.com/minio/minio-go"
)

var (
	ErrPathNotFound   = errors.New("PathNotFound")
	ErrBucketNotFound = errors.New("BucketNotFound")
)

func convertError(err error) error {
	switch minio.ToErrorResponse(err).Code {

	case "NoSuchKey":
		return ErrPathNotFound

	case "NoSuchBucket":
		return ErrBucketNotFound

	default:
		return err
	}
}
