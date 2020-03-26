package objstore

import "testing"

func TestCPGEUStage0UploadDownload(t *testing.T) {
	cl := NewCPGEUStage0Client(NewClientForTesting(), testBucket)

	if err := cl.Upload("files/d", "goat", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	if err := cl.Download("goat", "2019-01-01.00", "files/out/2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	if !pathsMatch("files/d", "files/out/2019-01-01.00") {
		t.Fatal("files/d")
	}
}

func TestCPGEUStage0ArchiveUnarchive(t *testing.T) {
	cl := NewCPGEUStage0Client(NewClientForTesting(), testBucket)

	if err := cl.Upload("files/d", "goat", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	if err := cl.Archive("goat", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	if err := cl.Archive("goat", "2019-01-01.00"); err != ErrPathNotFound {
		t.Fatal(err)
	}

	if err := cl.Unarchive("goat", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	if err := cl.Unarchive("goat", "2019-01-01.00"); err != ErrPathNotFound {
		t.Fatal(err)
	}

	if err := cl.Download("goat", "2019-01-01.00", "files/out/2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	if !pathsMatch("files/d", "files/out/2019-01-01.00") {
		t.Fatal("files/d")
	}
}

func TestCPGEUStage0ListVendors(t *testing.T) {
	cl := NewCPGEUStage0Client(NewClientForTesting(), testBucket)

	if err := cl.Upload("files/d", "goat", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	vendors, err := cl.ListVendors()
	if err != nil {
		t.Fatal(err)
	}
	if len(vendors) != 1 || vendors[0] != "goat" {
		t.Fatal(vendors)
	}

	if err := cl.Upload("files/d", "toad", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	vendors, err = cl.ListVendors()
	if err != nil {
		t.Fatal(err)
	}
	if len(vendors) != 2 || vendors[0] != "goat" || vendors[1] != "toad" {
		t.Fatal(vendors)
	}
}

func TestCPGEUStage0ListVersions(t *testing.T) {
	cl := NewCPGEUStage0Client(NewClientForTesting(), testBucket)

	if err := cl.Upload("files/d", "goat", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	if err := cl.Upload("files/d", "goat", "2019-01-02.00"); err != nil {
		t.Fatal(err)
	}

	if err := cl.Upload("files/d", "goat", "2019-01-02.01"); err != nil {
		t.Fatal(err)
	}

	if err := cl.Archive("goat", "2019-01-02.00"); err != nil {
		t.Fatal(err)
	}

	l, err := cl.ListVersions("goat")
	if err != nil {
		t.Fatal(err)
	}
	if len(l) != 3 {
		t.Fatal(l)
	}

	if l[0].Name != "2019-01-01.00" || l[0].Archived {
		t.Fatal(l[0])
	}
	if l[1].Name != "2019-01-02.00" || !l[1].Archived {
		t.Fatal(l[1])
	}
	if l[2].Name != "2019-01-02.01" || l[2].Archived {
		t.Fatal(l[2])
	}
}

func TestCPGEUStage0UploadErrors(t *testing.T) {
	cl := NewCPGEUStage0Client(NewClientForTesting(), testBucket)

	if err := cl.Upload("files/d", "x", "2019-01-01.00"); err != ErrInvalidVendor {
		t.Fatal(err)
	}
	if err := cl.Upload("files/d", "goat", "201-01-01.00"); err != ErrInvalidVersion {
		t.Fatal(err)
	}
}

func TestCPGEUStage0Delete(t *testing.T) {
	cl := NewCPGEUStage0Client(NewClientForTesting(), testBucket)

	if err := cl.Upload("files/d", "goat", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	if err := cl.Archive("goat", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	if err := cl.Delete("goat", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	if err := cl.Download("goat", "2019-01-01.00", "files/out/2019-01-01.00"); err != ErrPathNotFound {
		t.Fatal(err)
	}
}
