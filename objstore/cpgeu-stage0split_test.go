package objstore

import "testing"

func TestCPGEUStage0SplitUploadDownload(t *testing.T) {
	cl := NewCPGEUStage0SplitClient(NewClientForTesting(), testBucket)

	err := cl.Upload("files/in.txt", "goat", "2019-01-01.00", "2019-01-02")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Download("goat", "2019-01-01.00", "2019-01-02", "files/out/x.txt")
	if err != nil {
		t.Fatal(err)
	}

	if !pathsMatch("files/in.txt", "files/out/x.txt") {
		t.Fatal("files/in.txt")
	}
}

func TestCPGEUStage0SplitListVendors(t *testing.T) {
	cl := NewCPGEUStage0SplitClient(NewClientForTesting(), testBucket)

	err := cl.Upload("files/in.txt", "goat", "2019-01-01.00", "2019-01-02")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Upload("files/in.txt", "dingo", "2019-01-01.00", "2019-01-02")
	if err != nil {
		t.Fatal(err)
	}

	l, err := cl.ListVendors()
	if err != nil {
		t.Fatal(err)
	}
	if len(l) != 2 || l[0] != "dingo" || l[1] != "goat" {
		t.Fatal(l)
	}
}

func TestCPGEUStage0SplitListVersions(t *testing.T) {
	cl := NewCPGEUStage0SplitClient(NewClientForTesting(), testBucket)

	err := cl.Upload("files/in.txt", "goat", "2019-01-03.00", "2019-01-02")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Upload("files/in.txt", "goat", "2019-01-02.00", "2019-01-02")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Upload("files/in.txt", "goat", "2019-01-01.00", "2019-01-03")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Upload("files/in.txt", "dingo", "2019-01-01.00", "2019-01-02")
	if err != nil {
		t.Fatal(err)
	}

	l, err := cl.ListVersions("goat")
	if err != nil {
		t.Fatal(err)
	}
	if len(l) != 3 {
		t.Fatal(l)
	}

	if l[0] != "2019-01-01.00" || l[1] != "2019-01-02.00" || l[2] != "2019-01-03.00" {
		t.Fatal(l)
	}
}

func TestCPGEUStage0SplitListDays(t *testing.T) {
	cl := NewCPGEUStage0SplitClient(NewClientForTesting(), testBucket)

	err := cl.Upload("files/in.txt", "goat", "2019-01-03.00", "2019-01-02")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Upload("files/in.txt", "goat", "2019-01-03.00", "2019-01-03")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Upload("files/in.txt", "goat", "2019-01-03.00", "2019-01-01")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Upload("files/in.txt", "goat", "2019-01-03.01", "2019-01-08")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Upload("files/in.txt", "dingo", "2019-01-01.00", "2019-01-02")
	if err != nil {
		t.Fatal(err)
	}

	l, err := cl.ListDays("goat", "2019-01-03.00")
	if err != nil {
		t.Fatal(err)
	}
	if len(l) != 3 {
		t.Fatal(l)
	}

	if l[0] != "2019-01-01" || l[1] != "2019-01-02" || l[2] != "2019-01-03" {
		t.Fatal(l)
	}
}

func TestCPGEUStage0SplitListAll(t *testing.T) {
	cl := NewCPGEUStage0SplitClient(NewClientForTesting(), testBucket)

	err := cl.Upload("files/in.txt", "goat", "2019-01-03.00", "2019-01-02")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Upload("files/in.txt", "goat", "2019-01-03.00", "2019-01-03")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Upload("files/in.txt", "goat", "2019-01-03.00", "2019-01-01")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Upload("files/in.txt", "dingo", "2019-01-03.01", "2019-01-08")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Upload("files/in.txt", "dingo", "2019-01-01.00", "2019-01-02")
	if err != nil {
		t.Fatal(err)
	}

	l, err := cl.ListAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(l) != 5 {
		t.Fatal(l)
	}

	ok := l[0].Vendor == "goat" &&
		l[0].Version == "2019-01-03.00" &&
		l[0].Day == "2019-01-01" &&
		l[4].Vendor == "dingo" &&
		l[4].Version == "2019-01-03.01" &&
		l[4].Day == "2019-01-08"
	if !ok {
		t.Fatal(l)
	}
}

func TestCPGEUStage0SplitDelete(t *testing.T) {
	cl := NewCPGEUStage0SplitClient(NewClientForTesting(), testBucket)

	if err := cl.Upload("files/in.txt", "goat", "2019-01-01.00", "2018-01-02"); err != nil {
		t.Fatal(err)
	}

	if err := cl.Delete("goat", "2019-01-01.00", "2018-01-02"); err != nil {
		t.Fatal(err)
	}

	if err := cl.Download("goat", "2019-01-01.00", "2018-01-02", "files/out/2019-01-01.00"); err != ErrPathNotFound {
		t.Fatal(err)
	}
}

func TestCPGEUStage0SplitDeleteAll(t *testing.T) {
	cl := NewCPGEUStage0SplitClient(NewClientForTesting(), testBucket)

	err := cl.Upload("files/in.txt", "goat", "2019-01-03.00", "2019-01-02")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Upload("files/in.txt", "goat", "2019-01-03.00", "2019-01-03")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Upload("files/in.txt", "goat", "2019-01-03.00", "2019-01-01")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Upload("files/in.txt", "dingo", "2019-01-03.01", "2019-01-08")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Upload("files/in.txt", "dingo", "2019-01-01.00", "2019-01-02")
	if err != nil {
		t.Fatal(err)
	}

	if err = cl.DeleteAll(); err != nil {
		t.Fatal(err)
	}

	l, err := cl.ListAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(l) != 0 {
		t.Fatal(l)
	}
}
