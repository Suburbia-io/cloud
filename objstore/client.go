package objstore

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"log"
	"os"
	"path/filepath"

	minio "github.com/minio/minio-go"
)

var (
	defaultHost   = os.Getenv("SB_OBJSTORE_HOST")
	defaultKey    = os.Getenv("SB_OBJSTORE_KEY")
	defaultSecret = os.Getenv("SB_OBJSTORE_SECRET")
	defaultEncKey = []byte(os.Getenv("SB_OBJSTORE_ENC_KEY"))
)

type Client struct {
	Host   string // Default from environment: SB_OBJSTORE_HOST
	Key    string // Default from environment: SB_OBJSTORE_KEY
	Secret string // Default from environment: SB_OBJSTORE_SECRET
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

func (cl *Client) Put(r io.Reader, bucket, rPath string) error {
	enc, err := encryptReader(cl.EncKey, r)
	if err != nil {
		return err
	}

	_, err = cl.cl.PutObject(bucket, rPath, enc, -1, minio.PutObjectOptions{})
	if err != nil {
		log.Printf("Failed to put object %s/%s: %v", bucket, rPath, err)
	}
	return convertError(err)
}

// ----------------------------------------------------------------------------

// PutGZ: Like Put, but compresses the data stream before sending.
func (cl *Client) PutGZ(r io.Reader, bucket, rPath string) error {
	return cl.Put(gzipReader(r), bucket, rPath)
}

// ----------------------------------------------------------------------------

func (cl *Client) PutFile(lPath, bucket, rPath string) error {
	f, err := os.Open(lPath)
	if err != nil {
		log.Printf("Failed to open file: %v", lPath)
		return err
	}
	defer f.Close()
	return cl.Put(f, bucket, rPath)
}

// ----------------------------------------------------------------------------

func (cl *Client) PutFileGZ(lPath, bucket, rPath string) error {
	f, err := os.Open(lPath)
	if err != nil {
		log.Printf("Failed to open file: %v", lPath)
		return err
	}
	defer f.Close()

	return cl.PutGZ(f, bucket, rPath)
}

// ----------------------------------------------------------------------------

func (cl *Client) PutDirTarGZ(lPath, bucket, rPath string) error {
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
			if _, err = io.Copy(tw, fSrc); err != nil {
				log.Printf("Failed to write file to tar archive: %v", err)
				w.CloseWithError(err)
				return
			}

			// Ignore close error.
			fSrc.Close()
		}

	}()
	return cl.PutGZ(r, bucket, rPath)
}

// ----------------------------------------------------------------------------

func (cl *Client) Get(bucket, rPath string) (io.ReadCloser, error) {
	obj, err := cl.cl.GetObject(bucket, rPath, minio.GetObjectOptions{})
	if err != nil {
		log.Printf("Failed to get object: %v", err)
		return nil, convertError(err)
	}

	r, err := decryptReader(cl.EncKey, obj)
	if err != nil {
		return nil, convertError(err)
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
func (cl *Client) GetGZ(bucket, rPath string) (io.ReadCloser, error) {
	r, err := cl.Get(bucket, rPath)
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

func (cl *Client) GetFile(bucket, rPath, lPath string) error {
	r, err := cl.Get(bucket, rPath)
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

func (cl *Client) GetFileGZ(bucket, rPath, lPath string) error {
	r, err := cl.GetGZ(bucket, rPath)
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

func (cl *Client) GetDirTarGZ(bucket, rPath, lPath string) error {
	f, err := cl.GetGZ(bucket, rPath)
	if err != nil {
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

func (cl *Client) Delete(bucket string, rPaths ...string) error {
	rPathCh := make(chan string, len(rPaths))
	for _, s := range rPaths {
		rPathCh <- s
	}
	close(rPathCh)

	var err error
	for rErr := range cl.cl.RemoveObjects(bucket, rPathCh) {
		if rErr.Err != nil {
			log.Printf("Failed to delete object %s: %v", rErr.ObjectName, rErr.Err)
			if err == nil {
				err = convertError(rErr.Err)
			}
		}
	}
	return err
}

// ----------------------------------------------------------------------------

func (cl *Client) List(
	bucket,
	prefix string,
	recursive bool,
) (
	[]FileInfo,
	error,
) {
	// Create a done channel to control 'ListObjectsV2' go routine.
	doneCh := make(chan struct{})

	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	l := []FileInfo{}
	objectCh := cl.cl.ListObjectsV2(bucket, prefix, recursive, doneCh)
	for obj := range objectCh {
		if obj.Err != nil {
			log.Printf("Error listing objects: %v", obj.Err)
			return nil, convertError(obj.Err)
		}
		l = append(l, FileInfo{
			Name:    obj.Key,
			ModTime: obj.LastModified.UTC(),
			Size:    obj.Size,
		})
	}

	return l, nil
}

func (cl *Client) ListNames(bucket, prefix string) ([]string, error) {
	l, err := cl.List(bucket, prefix, false)
	if err != nil {
		return nil, err
	}
	ls := make([]string, len(l))
	for i := range ls {
		ls[i] = l[i].Name
	}
	return ls, nil
}

func (cl *Client) ListBaseNames(bucket, prefix string) ([]string, error) {
	l, err := cl.ListNames(bucket, prefix)
	if err != nil {
		return nil, err
	}
	for i := range l {
		l[i] = Base(l[i])
	}
	return l, nil
}

// ----------------------------------------------------------------------------

func (cl *Client) Stat(bucket, rPath string) (FileInfo, error) {
	info, err := cl.cl.StatObject(bucket, rPath, minio.StatObjectOptions{})
	if err != nil {
		log.Printf("Failed to stat object: %v", err)
		return FileInfo{}, convertError(err)
	}
	return FileInfo{
		Name:    info.Key,
		ModTime: info.LastModified.UTC(),
		Size:    info.Size,
	}, nil
}

// ----------------------------------------------------------------------------

func (cl *Client) Copy(bucket, srcPath, dstPath string) error {
	// Source object.
	src := minio.NewSourceInfo(bucket, srcPath, nil)

	// Destination object.
	dst, err := minio.NewDestinationInfo(bucket, dstPath, nil, nil)
	if err != nil {
		log.Printf("Failed to create destination: %v", err)
		return convertError(err)
	}

	// Copy object call.
	if err = cl.cl.CopyObject(dst, src); err != nil {
		log.Printf("Failed to copy %s -> %s: %v", srcPath, dstPath, err)
		return convertError(err)
	}

	return nil
}

// ----------------------------------------------------------------------------
