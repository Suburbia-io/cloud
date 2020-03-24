package objstore

import (
	"compress/gzip"
	"io"
)

// Wrap the reader in a new reading returning compressed data.
func gzipReader(rRaw io.Reader) io.Reader {
	r, w := io.Pipe()
	go func() {
		defer w.Close()

		gz, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
		defer gz.Close()
		if err != nil {
			w.CloseWithError(err)
		}

		// TODO: Can this return an error?
		io.Copy(gz, rRaw)
	}()
	return r
}
