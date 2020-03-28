package objstore

import (
	"bytes"
	"log"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type CPGEUStage1PreClient struct {
	bucket       string
	buildVersion string
	cl           *Client
}

// ----------------------------------------------------------------------------

func NewCPGEUStage1PreClient(
	cl *Client,
	bucket string,
	buildVersion string,
) CPGEUStage1PreClient {
	return CPGEUStage1PreClient{
		bucket:       bucket,
		buildVersion: buildVersion,
		cl:           cl,
	}
}

// ----------------------------------------------------------------------------

func (c CPGEUStage1PreClient) shadowPath(vendor, version string) string {
	return Join(c.buildVersion, "stage1pre.shadow", vendor, version)
}

func (c CPGEUStage1PreClient) putShadow(vendor, version string) error {
	rPath := c.shadowPath(vendor, version)
	r := bytes.NewBuffer([]byte{0})
	return c.cl.Put(r, c.bucket, rPath)
}

// ----------------------------------------------------------------------------

func (c CPGEUStage1PreClient) hasShadow(vendor, version string) (bool, error) {
	rPath := c.shadowPath(vendor, version)
	r, err := c.cl.Get(c.bucket, rPath)
	switch err {
	case nil:
		r.Close()
		return true, nil
	case ErrPathNotFound:
		return false, nil
	default:
		return false, err
	}
}

// ----------------------------------------------------------------------------

func (c CPGEUStage1PreClient) delShadow(vendor, version string) error {
	rPath := c.shadowPath(vendor, version)
	return c.cl.Delete(c.bucket, rPath)
}

// ----------------------------------------------------------------------------

// Returned map key is <vendor>.<version>.
func (c CPGEUStage1PreClient) getShadowAll() (map[string]struct{}, error) {
	m := map[string]struct{}{}
	rPath := Join(c.buildVersion, "stage1pre.shadow") + "/"

	infos, err := c.cl.List(c.bucket, rPath, true)
	if err != nil {
		return m, err
	}

	m = make(map[string]struct{}, len(infos))
	for _, info := range infos {
		parts := strings.Split(info.Name, "/")
		if len(parts) != 4 {
			log.Printf("Invalid name in stage1pre shadow list: %s", info.Name)
			continue
		}
		key := parts[2] + "." + parts[3]
		m[key] = struct{}{}
	}

	return m, nil
}

// ----------------------------------------------------------------------------

func (c CPGEUStage1PreClient) objPath(vendor, version, day string) string {
	return Join(c.buildVersion, "stage1pre", vendor, version, day+".tar.gz")
}

// splitDir should contain sub-directories of the form YYYY-MM-DD containing
// tables.
func (c CPGEUStage1PreClient) Upload(splitDir, vendor, version string) error {
	if err := validateCPGEUVendor(vendor); err != nil {
		return err
	}
	if err := validateVersion(version); err != nil {
		return err
	}

	if err := c.Delete(vendor, version); err != nil {
		return err
	}

	srcDirs, err := filepath.Glob(filepath.Join(splitDir, "????-??-??"))
	if err != nil {
		return err
	}

	for _, srcDir := range srcDirs {
		day := filepath.Base(srcDir)
		if err := validateDay(day); err != nil {
			return err
		}
		rPath := c.objPath(vendor, version, day)
		if err := c.cl.PutDirTarGZ(srcDir, c.bucket, rPath); err != nil {
			return err
		}
	}

	return c.putShadow(vendor, version)
}

// ----------------------------------------------------------------------------

func (c CPGEUStage1PreClient) Delete(vendor, version string) error {
	if err := validateCPGEUVendor(vendor); err != nil {
		return err
	}
	if err := validateVersion(version); err != nil {
		return err
	}

	if err := c.delShadow(vendor, version); err != nil {
		return err
	}

	rPath := Join(c.buildVersion, "stage1pre", vendor, version) + "/"
	infos, err := c.cl.List(c.bucket, rPath, true)
	if err != nil {
		return err
	}

	return c.cl.Delete(c.bucket, fileInfoList(infos).Names()...)
}

// ----------------------------------------------------------------------------

func (c CPGEUStage1PreClient) ListVendors() ([]string, error) {
	rPath := filepath.Join(c.buildVersion, "stage1pre.shadow") + "/"

	infos, err := c.cl.List(c.bucket, rPath, false)
	if err != nil {
		return nil, err
	}
	return fileInfoList(infos).BaseNames(), nil
}

// ----------------------------------------------------------------------------

func (c CPGEUStage1PreClient) ListVersions(vendor string) ([]string, error) {
	rPath := filepath.Join(c.buildVersion, "stage1pre.shadow", vendor) + "/"

	infos, err := c.cl.List(c.bucket, rPath, false)
	if err != nil {
		return nil, err
	}
	return fileInfoList(infos).BaseNames(), nil
}

// ----------------------------------------------------------------------------

func (c CPGEUStage1PreClient) ListDays(vendor, version string) ([]string, error) {
	ok, err := c.hasShadow(vendor, version)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrPathNotFound
	}

	rPath := filepath.Join(c.buildVersion, "stage1pre", vendor, version) + "/"

	infos, err := c.cl.List(c.bucket, rPath, false)
	if err != nil {
		return nil, err
	}

	l := make([]string, 0, len(infos))
	for i, name := range fileInfoList(infos).BaseNames() {
		if len(name) != 17 {
			log.Printf("Invalid day stage1pre path: %s", infos[i].Name)
			continue
		}
		l = append(l, name[:10])
	}

	return l, nil
}

// ----------------------------------------------------------------------------

type CPGEUStage1PreDay struct {
	Vendor  string
	Version string
	Day     string
	ModTime time.Time
}

func (c CPGEUStage1PreClient) ListAll() (l []CPGEUStage1PreDay, err error) {
	m, err := c.getShadowAll()
	if err != nil {
		return nil, err
	}

	rPath := filepath.Join(c.buildVersion, "stage1pre") + "/"
	infos, err := c.cl.List(c.bucket, rPath, true)
	if err != nil {
		return nil, err
	}

	l = make([]CPGEUStage1PreDay, 0, len(infos))
	for _, info := range infos {
		parts := strings.Split(info.Name, "/")
		if len(parts) != 5 || len(parts[4]) != 17 {
			log.Printf("Invalid stage1pre path: %s", info.Name)
			continue
		}
		vendor := parts[2]
		version := parts[3]
		day := parts[4][:10]

		if _, ok := m[vendor+"."+version]; !ok {
			continue
		}

		l = append(l, CPGEUStage1PreDay{
			Vendor:  vendor,
			Version: version,
			Day:     day,
			ModTime: info.ModTime,
		})
	}

	sort.Slice(l, func(i, j int) bool {
		if l[i].Vendor != l[j].Vendor {
			return l[i].Vendor < l[j].Vendor
		}
		if l[i].Version != l[j].Version {
			return l[i].Version < l[j].Version
		}
		return l[i].Day < l[j].Day
	})

	return l, nil
}

// ----------------------------------------------------------------------------

func (c CPGEUStage1PreClient) Download(
	vendor string,
	version string,
	day string,
	dstDir string,
) error {

	rPath := c.objPath(vendor, version, day)
	return c.cl.GetDirTarGZ(c.bucket, rPath, dstDir)
}
