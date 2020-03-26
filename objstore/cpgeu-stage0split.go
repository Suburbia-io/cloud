package objstore

import (
	"log"
	"sort"
	"strings"
	"time"
)

type CPGEUStage0SplitClient struct {
	bucket string
	cl     *Client
}

// ----------------------------------------------------------------------------

func NewCPGEUStage0SplitClient(
	cl *Client,
	bucket string,
) CPGEUStage0SplitClient {
	return CPGEUStage0SplitClient{
		bucket: bucket,
		cl:     cl,
	}
}

// ----------------------------------------------------------------------------

func (c CPGEUStage0SplitClient) ListVendors() ([]string, error) {
	rPath := "stage0split/"
	infos, err := c.cl.List(c.bucket, rPath, false)
	if err != nil {
		return nil, err
	}
	l := make([]string, len(infos))
	for i, info := range infos {
		l[i] = Base(info.Name)
	}
	sort.Strings(l)
	return l, nil
}

// ----------------------------------------------------------------------------

func (c CPGEUStage0SplitClient) ListVersions(vendor string) ([]string, error) {
	rPath := Join("stage0split", vendor) + "/"

	infos, err := c.cl.List(c.bucket, rPath, false)
	if err != nil {
		return nil, err
	}
	l := make([]string, len(infos))
	for i, info := range infos {
		name := Base(info.Name)
		if len(name) != 13 {
			log.Printf("Invalid name in version list: %s", info.Name)
			continue
		}
		l[i] = name
	}
	sort.Strings(l)
	return l, nil
}

// ----------------------------------------------------------------------------

func (c CPGEUStage0SplitClient) ListDays(
	vendor string,
	version string,
) (
	[]string,
	error,
) {
	rPath := Join("stage0split", vendor, version) + "/"

	infos, err := c.cl.List(c.bucket, rPath, false)
	if err != nil {
		return nil, err
	}
	l := make([]string, len(infos))
	for i, info := range infos {
		name := Base(info.Name)
		if len(name) < 10 {
			log.Printf("Invalid name in day list: %s", info.Name)
			continue
		}
		l[i] = name[:10]
	}
	sort.Strings(l)
	return l, nil
}

// ----------------------------------------------------------------------------

type CPGEUStage0SplitDay struct {
	Vendor  string
	Version string
	Day     string
	ModTime time.Time
}

func (c CPGEUStage0SplitClient) ListAll() ([]CPGEUStage0SplitDay, error) {
	rPath := "stage0split/"
	infos, err := c.cl.List(c.bucket, rPath, true)
	if err != nil {
		return nil, err
	}
	l := make([]CPGEUStage0SplitDay, len(infos))
	for i, info := range infos {
		parts := strings.Split(info.Name, "/")
		if len(parts) != 4 || len(parts[3]) < 10 {
			log.Printf("Invalid path is split list: %v", info.Name)
			continue
		}

		l[i] = CPGEUStage0SplitDay{
			Vendor:  parts[1],
			Version: parts[2],
			Day:     parts[3][:10],
			ModTime: info.ModTime,
		}
	}

	sort.Slice(l, func(i, j int) bool {
		if l[i].Day != l[j].Day {
			return l[i].Day < l[j].Day
		}
		if l[i].Vendor != l[j].Vendor {
			return l[i].Vendor < l[j].Vendor
		}
		return l[i].Version < l[j].Version
	})

	return l, nil
}

// ----------------------------------------------------------------------------

func (c CPGEUStage0SplitClient) Upload(
	srcPath string, // Must be a csv file.
	vendor string,
	version string,
	day string,
) error {
	if err := validateCPGEUVendor(vendor); err != nil {
		return ErrInvalidVendor
	}
	if err := validateVersion(version); err != nil {
		return ErrInvalidVersion
	}
	if err := validateDay(day); err != nil {
		return ErrInvalidDay
	}

	rPath := Join("stage0split", vendor, version, day+".csv.gz")
	return c.cl.PutFileGZ(srcPath, c.bucket, rPath)
}

// ----------------------------------------------------------------------------

func (c CPGEUStage0SplitClient) Download(
	vendor string,
	version string,
	day string,
	dstPath string,
) error {
	rPath := Join("stage0split", vendor, version, day+".csv.gz")
	return c.cl.GetFileGZ(c.bucket, rPath, dstPath)
}

// ----------------------------------------------------------------------------

func (c CPGEUStage0SplitClient) Delete(vendor, version, day string) error {
	rPath := Join("stage0split", vendor, version, day+".csv.gz")
	return c.cl.Delete(c.bucket, rPath)
}

// ----------------------------------------------------------------------------

func (c CPGEUStage0SplitClient) DeleteAll() error {
	l, err := c.ListAll()
	if err != nil {
		return err
	}
	paths := make([]string, len(l))
	for i, info := range l {
		paths[i] = Join("stage0split", info.Vendor, info.Version, info.Day+".csv.gz")
	}
	return c.cl.Delete(c.bucket, paths...)
}
