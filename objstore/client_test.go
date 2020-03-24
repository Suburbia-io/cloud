package objstore

import "testing"

func TestClientPutFileGetFile(t *testing.T) {
	cl := Client{}
	if err := cl.Connect(); err != nil {
		t.Fatal(err)
	}

	rPath := "x/y/z/out.txt"

	err := cl.PutFileGZ("files/in.txt", rPath)
	if err != nil {
		t.Fatal(err)
	}

	err = cl.Copy(rPath, rPath+"x")
	if err != nil {
		t.Fatal(err)
	}

	err = cl.GetFileGZ(rPath+"x", "files/out.txt")
	if err != nil {
		t.Fatal(err)
	}

	// TODO: compare files!

	if err = cl.Delete(rPath); err != nil {
		t.Fatal(err)
	}
}
