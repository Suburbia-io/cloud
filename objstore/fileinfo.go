package objstore

import (
	"sort"
	"time"
)

type FileInfo struct {
	Name    string    // Full path to the object.
	ModTime time.Time // Modification time.
	Size    int64     // Size in storage.
}

type fileInfoList []FileInfo

func (infos fileInfoList) Names() []string {
	paths := make([]string, len(infos))
	for i, info := range infos {
		paths[i] = info.Name
	}
	sort.Strings(paths)
	return paths
}

func (infos fileInfoList) BaseNames() []string {
	bns := make([]string, len(infos))
	for i, info := range infos {
		bns[i] = Base(info.Name)
	}
	sort.Strings(bns)
	return bns
}
