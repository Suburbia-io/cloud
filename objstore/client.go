package objstore

// TODO: PutDirTarGZ
// TODO: GetDirTarGZ

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	minio "github.com/minio/minio-go"
)

var (
	defaultHost   = os.Getenv("SB_OBJSTORE_HOST")
	defaultKey    = os.Getenv("SB_OBJSTORE_KEY")
	defaultSecret = os.Getenv("SB_OBJSTORE_SECRET")
	defaultBucket = os.Getenv("SB_OBJSTORE_BUCKET")
	defaultEncKey = []byte(os.Getenv("SB_OBJSTORE_ENC_KEY"))
)

type Client struct {
	Host   string // Default from environment: SB_OBJSTORE_HOST
	Key    string // Default from environment: SB_OBJSTORE_KEY
	Secret string // Default from environment: SB_OBJSTORE_SECRET
	Bucket string // Default from environment: SB_OBJSTORE_BUCKET
	EncKey []byte // Default from environment: SB_OBJSTORE_ENC_KEY

	cl *minio.Client
}

// ----------------------------------------------------------------------------

func (cl *Client) Connect() (err error) {
	if cl.cl != nil {
		cl.cl = nil
	}
	if cl.Host == "" {
		cl.Host = defaultHost
	}
	if cl.Key == "" {
		cl.Key = defaultKey
	}
	if cl.Secret == "" {
		cl.Secret = defaultSecret
	}
	if cl.Bucket == "" {
		cl.Bucket = defaultBucket
	}
	if len(cl.EncKey) == 0 {
		cl.EncKey = defaultEncKey
	}

	cl.cl, err = minio.New(cl.Host, cl.Key, cl.Secret, true)
	if err != nil {
		log.Printf("Failed to connect to object store: %v", err)
	}
	return err
}

// ----------------------------------------------------------------------------

func (cl *Client) Put(r io.Reader, rPath string) error {
	enc, err := encryptReader(cl.EncKey, r)
	if err != nil {
		return err
	}

	_, err = cl.cl.PutObject(cl.Bucket, rPath, enc, -1, minio.PutObjectOptions{})
	if err != nil {
		log.Printf("Failed to put object %s/%s: %v", cl.Bucket, rPath, err)
	}
	return err
}

// ----------------------------------------------------------------------------

// PutGZ: Like Put, but compresses the data stream before sending.
func (cl *Client) PutGZ(r io.Reader, rPath string) error {
	return cl.Put(gzipReader(r), rPath)
}

// ----------------------------------------------------------------------------

func (cl *Client) PutFile(lPath, rPath string) error {
	f, err := os.Open(lPath)
	if err != nil {
		log.Printf("Failed to open file: %v", lPath)
		return err
	}
	defer f.Close()
	return cl.Put(f, rPath)
}

// ----------------------------------------------------------------------------

func (cl *Client) PutFileGZ(lPath, rPath string) error {
	f, err := os.Open(lPath)
	if err != nil {
		log.Printf("Failed to open file: %v", lPath)
		return err
	}
	defer f.Close()

	return cl.Put(gzipReader(f), rPath)
}

// ----------------------------------------------------------------------------

func (cl *Client) PutDirTarGZ(lPath, rPath string) error {
	r, w := io.Pipe()
	go func() {
		defer w.Close()

		paths, err := filepath.Glob(filepath.Join(lPath, "*"))
		if err != nil {
			log.Printf("Failed to list local files: %v", err)
			w.CloseWithError(err)
			return
		}

		tw := tar.NewWriter(w)
		defer tw.Close()

		for _, path := range paths {
			fSrc, err := os.Open(path)
			if err != nil {
				log.Printf("Failed to open file %s: %v", path, err)
				w.CloseWithError(err)
				return
			}
			fInfo, err := fSrc.Stat()
			if err != nil {
				log.Printf("Failed to stat file %s: %v", path, err)
				w.CloseWithError(err)
				return
			}

			// Write the header.
			err = tw.WriteHeader(&tar.Header{
				Typeflag: tar.TypeReg,
				Name:     filepath.Base(path),
				Size:     fInfo.Size(),
				Mode:     int64(fInfo.Mode()),
			})
			if err != nil {
				log.Printf("Failed to write tar header: %v", err)
				w.CloseWithError(err)
				return
			}

			// Write the data.
			if _, err = io.Copy(w, fSrc); err != nil {
				log.Printf("Failed to write file to tar archive: %v", err)
				w.CloseWithError(err)
				return
			}

			// Ignore close error.
			fSrc.Close()
		}

	}()
	return cl.PutGZ(r, rPath)
}

// ----------------------------------------------------------------------------

func (cl *Client) Get(rPath string) (io.ReadCloser, error) {
	obj, err := cl.cl.GetObject(cl.Bucket, rPath, minio.GetObjectOptions{})
	if err != nil {
		log.Printf("Failed to get object: %v", err)
		return nil, err
	}

	r, err := decryptReader(cl.EncKey, obj)
	if err != nil {
		return nil, err
	}

	return struct {
		io.Reader
		io.Closer
	}{
		Reader: r,
		Closer: obj,
	}, nil
}

// ----------------------------------------------------------------------------

// GetGZ: like Get, but decompresses the data stream.
func (cl *Client) GetGZ(rPath string) (io.ReadCloser, error) {
	r, err := cl.Get(rPath)
	if err != nil {
		return nil, err
	}
	gr, err := gzip.NewReader(r)
	if err != nil {
		log.Printf("Failed to create gzip reader: %v", err)
		return nil, err
	}
	return struct {
		io.Reader
		io.Closer
	}{
		Reader: gr,
		Closer: r,
	}, nil
}

// ----------------------------------------------------------------------------

func (cl *Client) GetFile(rPath, lPath string) error {
	r, err := cl.Get(rPath)
	if err != nil {
		return err
	}
	defer r.Close()

	f, err := os.Create(lPath)
	if err != nil {
		log.Printf("Failed to create file: %v", err)
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, r); err != nil {
		log.Printf("Failed to copy object to file: %v", err)
		return err
	}
	return nil
}

// ----------------------------------------------------------------------------

func (cl *Client) GetFileGZ(rPath, lPath string) error {
	r, err := cl.Get(rPath)
	if err != nil {
		return err
	}
	defer r.Close()

	gr, err := gzip.NewReader(r)
	if err != nil {
		log.Printf("Failed to create gzip reader: %v", err)
		return err
	}
	defer gr.Close()

	f, err := os.Create(lPath)
	if err != nil {
		log.Printf("Failed to create file: %v", err)
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, gr); err != nil {
		log.Printf("Failed to copy object to file: %v", err)
		return err
	}
	return nil
}

// ----------------------------------------------------------------------------

func (cl *Client) GetDirTarGZ(rPath, lPath string) error {
	f, err := cl.GetGZ(rPath)
	if err != nil {
		log.Printf("Failed to get object %s: %v", rPath, err)
		return err
	}
	defer f.Close()

	if err := os.RemoveAll(lPath); err != nil {
		log.Printf("Failed to remove local directory %s: %v", lPath, err)
		return err
	}

	if err := os.MkdirAll(lPath, 0700); err != nil {
		log.Printf("Failed to create local directory %s: %v", lPath, err)
		return err
	}

	r := tar.NewReader(f)

	for {
		header, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Failed to read tar header: %v", err)
			return err
		}

		dstPath := filepath.Join(lPath, header.Name)
		dst, err := os.Create(dstPath)

		if err != nil {
			log.Printf("Failed to create file %s: %v", dstPath, err)
			return err
		}

		if _, err = io.CopyN(dst, r, header.Size); err != nil {
			log.Printf("Failed to write file %s: %v", dstPath, err)
			return err
		}

		if err := dst.Close(); err != nil {
			log.Printf("Failed to close file %s: %v", dstPath, err)
			return err
		}
	}

	return nil
}

// ----------------------------------------------------------------------------

func (cl *Client) Delete(rPaths ...string) error {
	rPathCh := make(chan string, len(rPaths))
	for _, s := range rPaths {
		rPathCh <- s
	}
	close(rPathCh)

	var err error
	for rErr := range cl.cl.RemoveObjects(cl.Bucket, rPathCh) {
		if rErr.Err != nil {
			log.Printf("Failed to delete object %s: %v", rErr.ObjectName, rErr.Err)
			if err == nil {
				err = rErr.Err
			}
		}
	}
	return err
}

// ----------------------------------------------------------------------------

type FileInfo struct {
	Name    string    // Full path to the object.
	ModTime time.Time // Modification time.
	Size    int64     // Size in storage.
}

func (cl *Client) List(prefix string, recursive bool) ([]FileInfo, error) {

	// Create a done channel to control 'ListObjectsV2' go routine.
	doneCh := make(chan struct{})

	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	l := []FileInfo{}
	objectCh := cl.cl.ListObjectsV2(cl.Bucket, prefix, recursive, doneCh)
	for obj := range objectCh {
		if obj.Err != nil {
			log.Printf("Error listing objects: %v", obj.Err)
			return nil, obj.Err
		}
		l = append(l, FileInfo{
			Name:    obj.Key,
			ModTime: obj.LastModified.UTC(),
			Size:    obj.Size,
		})
	}

	return l, nil
}

// ----------------------------------------------------------------------------

func (cl *Client) Stat(rPath string) (FileInfo, error) {
	// TODO
}

// ----------------------------------------------------------------------------

func (cl *Client) Copy(srcPath, dstPath string) error {
	// Source object.
	src := minio.NewSourceInfo(cl.Bucket, srcPath, nil)

	// Destination object.
	dst, err := minio.NewDestinationInfo(cl.Bucket, dstPath, nil, nil)
	if err != nil {
		log.Printf("Failed to create destination: %v", err)
		return err
	}

	// Copy object call.
	if err = cl.cl.CopyObject(dst, src); err != nil {
		log.Printf("Failed to copy %s -> %s: %v", srcPath, dstPath, err)
		return err
	}

	return nil

}
