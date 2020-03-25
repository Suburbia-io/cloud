package objstore

import (
	"log"
	"strings"
)

type CPGEUStage0Info struct {
	Vendor   string
	Version  string
	Archived bool
}

// ----------------------------------------------------------------------------

type CPGEUStage0Client struct {
	bucket string
	cl     *Client
}

// ----------------------------------------------------------------------------

func NewCPGEUStage0Client(cl *Client, bucket string) CPGEUStage0Client {
	return CPGEUStage0Client{
		bucket: bucket,
		cl:     cl,
	}
}

// ----------------------------------------------------------------------------

func (c CPGEUStage0Client) ListVendors() ([]string, error) {
	rPath := "stage0/"
	infos, err := c.cl.List(c.bucket, rPath, false)
	if err != nil {
		return nil, err
	}
	l := make([]string, len(infos))
	for i, info := range infos {
		l[i] = Base(info.Name)
	}
	return l, nil
}

// ----------------------------------------------------------------------------

func (c CPGEUStage0Client) ListVersions(vendor string) ([]CPGEUStage0Info, error) {
	rPath := Join("stage0", vendor) + "/"

	infos, err := c.cl.List(c.bucket, rPath, false)
	if err != nil {
		return nil, err
	}
	l := make([]CPGEUStage0Info, len(infos))
	for i, info := range infos {
		name := Base(info.Name)
		if len(name) < 13 {
			log.Printf("Invalid name in version list: %s", info.Name)
			continue
		}
		l[i] = CPGEUStage0Info{
			Vendor:   vendor,
			Version:  name[:13],
			Archived: strings.HasSuffix(name, ".archived"),
		}
	}
	return l, nil
}

// ----------------------------------------------------------------------------

func (c CPGEUStage0Client) Upload(
	srcDir string,
	vendor string,
	version string,
) error {
	if err := c.validateVendor(vendor); err != nil {
		return ErrInvalidVendor
	}
	if err := validateVersion(version); err != nil {
		return ErrInvalidVersion
	}
	rPath := Join("stage0", vendor, version+".tar.gz")
	return c.cl.PutDirTarGZ(srcDir, c.bucket, rPath)
}

// ----------------------------------------------------------------------------

func (c CPGEUStage0Client) Download(
	vendor string,
	version string,
	dstDir string,
) error {
	rPath := Join("stage0", vendor, version+".tar.gz")
	return c.cl.GetDirTarGZ(c.bucket, rPath, dstDir)
}

// ----------------------------------------------------------------------------

func (c CPGEUStage0Client) Archive(vendor, version string) error {
	srcPath := Join("stage0", vendor, version+".tar.gz")
	dstPath := Join("stage0", vendor, version+".tar.gz.archived")
	if err := c.cl.Copy(c.bucket, srcPath, dstPath); err != nil {
		return err
	}
	return c.cl.Delete(c.bucket, srcPath)
}

// ----------------------------------------------------------------------------

func (c CPGEUStage0Client) Unarchive(vendor, version string) error {
	srcPath := Join("stage0", vendor, version+".tar.gz.archived")
	dstPath := Join("stage0", vendor, version+".tar.gz")
	if err := c.cl.Copy(c.bucket, srcPath, dstPath); err != nil {
		return err
	}
	return c.cl.Delete(c.bucket, srcPath)
}

// ----------------------------------------------------------------------------

// Must be archived first.
func (c CPGEUStage0Client) Delete(vendor, version string) error {
	rPath := Join("stage0", vendor, version+".tar.gz.archived")
	return c.cl.Delete(c.bucket, rPath)
}

// ----------------------------------------------------------------------------

func (CPGEUStage0Client) validateVendor(vendor string) error {
	switch vendor {
	case "chicken", "dingo", "goat", "toad":
		return nil
	default:
		return ErrInvalidVendor
	}
}
