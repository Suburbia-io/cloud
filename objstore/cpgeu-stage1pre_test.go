package objstore

import (
	"testing"
	"time"
)

func TestCPGEUStage1PrePutHasDelShadow(t *testing.T) {
	cl := NewCPGEUStage1PreClient(
		NewClientForTesting(),
		testBucket,
		"2020.01.test")

	if err := cl.putShadow("goat", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	ok, err := cl.hasShadow("goat", "2019-01-01.00")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal(ok)
	}

	ok, err = cl.hasShadow("goat", "2019-01-01.01")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal(ok)
	}

	if err := cl.delShadow("goat", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	ok, err = cl.hasShadow("goat", "2019-01-01.00")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal(ok)
	}
}

func TestCPGEUStage1PreGetShadowAll(t *testing.T) {
	cl := NewCPGEUStage1PreClient(
		NewClientForTesting(),
		testBucket,
		"2020.01.test")

	if err := cl.putShadow("goat", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	if err := cl.putShadow("goat", "2019-01-02.00"); err != nil {
		t.Fatal(err)
	}

	if err := cl.putShadow("toad", "2019-01-03.01"); err != nil {
		t.Fatal(err)
	}

	m, err := cl.getShadowAll()
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := m["goat.2019-01-01.00"]; !ok {
		t.Fatal(ok)
	}
	if _, ok := m["goat.2019-01-02.00"]; !ok {
		t.Fatal(ok)
	}
	if _, ok := m["toad.2019-01-03.01"]; !ok {
		t.Fatal(ok)
	}
}

func TestCPGEUStage1PreUploadDownload(t *testing.T) {
	cl := NewCPGEUStage1PreClient(
		NewClientForTesting(),
		testBucket,
		"2020.01.test")

	if err := cl.Upload("files/cpgeu-stage1pre/", "goat", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	if err := cl.Download("goat", "2019-01-01.00", "2019-01-03", "files/out/cpgeu.1"); err != nil {
		t.Fatal(err)
	}

	if !pathsMatch("files/cpgeu-stage1pre/2019-01-03", "files/out/cpgeu.1") {
		t.Fatal("files/out/cpgeu.1")
	}
}

func TestCPGEUStage1PreListVendors(t *testing.T) {
	cl := NewCPGEUStage1PreClient(
		NewClientForTesting(),
		testBucket,
		"2020.01.test")

	if err := cl.Upload("files/cpgeu-stage1pre/", "goat", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	if err := cl.Upload("files/cpgeu-stage1pre/", "toad", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	l, err := cl.ListVendors()
	if err != nil {
		t.Fatal(err)
	}

	if len(l) != 2 || l[0] != "goat" || l[1] != "toad" {
		t.Fatal(l)
	}

	if err := cl.delShadow("toad", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	l, err = cl.ListVendors()
	if err != nil {
		t.Fatal(err)
	}

	if len(l) != 1 || l[0] != "goat" {
		t.Fatal(l)
	}
}

func TestCPGEUStage1PreListVersions(t *testing.T) {
	cl := NewCPGEUStage1PreClient(
		NewClientForTesting(),
		testBucket,
		"2020.01.test")

	if err := cl.Upload("files/cpgeu-stage1pre/", "goat", "2019-01-03.00"); err != nil {
		t.Fatal(err)
	}

	if err := cl.Upload("files/cpgeu-stage1pre/", "goat", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	l, err := cl.ListVersions("goat")
	if err != nil {
		t.Fatal(err)
	}

	if len(l) != 2 || l[0] != "2019-01-01.00" || l[1] != "2019-01-03.00" {
		t.Fatal(l)
	}

	if err := cl.delShadow("goat", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	l, err = cl.ListVersions("goat")
	if err != nil {
		t.Fatal(err)
	}

	if len(l) != 1 || l[0] != "2019-01-03.00" {
		t.Fatal(l)
	}
}

func TestCPGEUStage1PreListDays(t *testing.T) {
	cl := NewCPGEUStage1PreClient(
		NewClientForTesting(),
		testBucket,
		"2020.01.test")

	if err := cl.Upload("files/cpgeu-stage1pre/", "goat", "2019-01-03.00"); err != nil {
		t.Fatal(err)
	}

	days, err := cl.ListDays("goat", "2019-01-03.00")
	if err != nil {
		t.Fatal(err)
	}

	if len(days) != 3 || days[0] != "2019-01-01" || days[1] != "2019-01-02" || days[2] != "2019-01-03" {
		t.Fatal(days)
	}
}

func TestCPGEUStage1PreListAll(t *testing.T) {
	cl := NewCPGEUStage1PreClient(
		NewClientForTesting(),
		testBucket,
		"2020.01.test")

	if err := cl.Upload("files/cpgeu-stage1pre/", "toad", "2019-02-01.01"); err != nil {
		t.Fatal(err)
	}

	if err := cl.Upload("files/cpgeu-stage1pre/", "goat", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	l, err := cl.ListAll()
	if err != nil {
		t.Fatal(err)
	}

	ok := len(l) == 6 &&
		l[0].Vendor == "goat" &&
		l[0].Version == "2019-01-01.00" &&
		l[0].Day == "2019-01-01" &&
		l[0].ModTime.After(time.Now().Add(-time.Minute)) &&
		l[5].Vendor == "toad" &&
		l[5].Version == "2019-02-01.01" &&
		l[5].Day == "2019-01-03" &&
		l[5].ModTime.After(time.Now().Add(-time.Minute))

	if !ok {
		t.Fatal(l)
	}

	if err := cl.delShadow("goat", "2019-01-01.00"); err != nil {
		t.Fatal(err)
	}

	l, err = cl.ListAll()
	if err != nil {
		t.Fatal(err)
	}

	ok = len(l) == 3 &&
		l[0].Vendor == "toad" &&
		l[0].Version == "2019-02-01.01" &&
		l[0].Day == "2019-01-01" &&
		l[0].ModTime.After(time.Now().Add(-time.Hour)) &&
		l[2].Vendor == "toad" &&
		l[2].Version == "2019-02-01.01" &&
		l[2].Day == "2019-01-03" &&
		l[2].ModTime.After(time.Now().Add(-time.Hour))

	if !ok {
		t.Fatal(l)
	}

}
