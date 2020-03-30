package objstore

import (
	"time"
)

type FileInfo struct {
	Name    string    // Full path to the object.
	ModTime time.Time // Modification time.
	Size    int64     // Size in storage.
}
