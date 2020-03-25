package objstore

import (
	"testing"
)

func TestClientPutFileGetFile(t *testing.T) {
	cl := NewClientForTesting()

	rPath := "x/y/z/out.txt"

	err := cl.PutFile("files/in.txt", testBucket, rPath)
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Copy(testBucket, rPath, rPath+"x")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.GetFile(testBucket, rPath+"x", "files/out/in.txt")
	if err != nil {
		t.Fatal(err)
	}

	if !pathsMatch("files/in.txt", "files/out/in.txt") {
		t.Fatal("files/in.txt")
	}
}

func TestClientPutFileGetFileGZ(t *testing.T) {
	cl := NewClientForTesting()

	rPath := "x/y/z/out.txt"

	err := cl.PutFileGZ("files/in.txt", testBucket, rPath)
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Copy(testBucket, rPath, rPath+"x")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.GetFileGZ(testBucket, rPath+"x", "files/out/in.txt")
	if err != nil {
		t.Fatal(err)
	}

	if !pathsMatch("files/in.txt", "files/out/in.txt") {
		t.Fatal("files/in.txt")
	}
}

func TestClientPutGetDirTarGZ(t *testing.T) {
	cl := NewClientForTesting()

	rPath := "q/r/s/my-dir.tar.gz"

	err := cl.PutDirTarGZ("files/d", testBucket, rPath)
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Copy(testBucket, rPath, rPath+"x")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.GetDirTarGZ(testBucket, rPath+"x", "files/out/d")
	if err != nil {
		t.Fatal(err)
	}

	if !pathsMatch("files/d", "files/out/d") {
		t.Fatal("files/d")
	}
}

func TestClientStat(t *testing.T) {
	cl := NewClientForTesting()

	rPath := "a/b/c/file.txt"

	err := cl.PutFileGZ("files/in.txt", testBucket, rPath)
	if err != nil {
		t.Fatal(err)
	}

	info, err := cl.Stat(testBucket, rPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != rPath {
		t.Fatal(info)
	}
}

func TestClientStatNotFound(t *testing.T) {
	cl := NewClientForTesting()

	rPath := "q/r/s/not-a-file"

	_, err := cl.Stat(testBucket, rPath)
	if err != ErrPathNotFound {
		t.Fatal(err)
	}
}
