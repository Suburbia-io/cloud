package objstore

import (
	"io"
	"log"
	"os"

	minio "github.com/minio/minio-go"
)

// ----------------------------------------------------------------------------

func (cl *Client) PutNC(r io.Reader, bucket, rPath string) error {
	_, err := cl.cl.PutObject(bucket, rPath, r, -1, minio.PutObjectOptions{
		PartSize: 1024 * 1024 * 64,
	})
	if err != nil {
		log.Printf("Failed to put object %s/%s: %v", bucket, rPath, err)
	}
	return convertError(err)
}

// ----------------------------------------------------------------------------

// PutGZ: Like PutNC, but compresses the data stream before sending.
func (cl *Client) PutNCGZ(r io.Reader, bucket, rPath string) error {
	return cl.PutNC(gzipReader(r), bucket, rPath)
}

// ----------------------------------------------------------------------------

func (cl *Client) PutNCFileGZ(lPath, bucket, rPath string) error {
	return cl.withRetry(func() error {
		return cl.putNCFileGZ(lPath, bucket, rPath)
	})
}

func (cl *Client) putNCFileGZ(lPath, bucket, rPath string) error {
	f, err := os.Open(lPath)
	if err != nil {
		log.Printf("Failed to open file: %v", lPath)
		return err
	}
	defer f.Close()

	return cl.PutNCGZ(f, bucket, rPath)
}
