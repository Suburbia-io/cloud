package objstore

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
)

var (
	testBucket = "suburbia-test"
)

func NewClientForTesting() *Client {
	cl := &Client{}
	if err := cl.Connect(); err != nil {
		panic(err)
	}
	cl.TestCleanup()
	return cl
}

func (cl *Client) TestCleanup() {
	l, err := cl.List(testBucket, "", true)
	if err != nil {
		panic(err)
	}
	delPaths := make([]string, len(l))
	for i := range l {
		delPaths[i] = l[i].Name
	}
	if err := cl.Delete(testBucket, delPaths...); err != nil {
		panic(err)
	}
	if err := os.RemoveAll("files/out"); err != nil {
		panic(err)
	}
	if err := os.MkdirAll("files/out", 0700); err != nil {
		panic(err)
	}
}

func pathsMatch(lhs, rhs string) bool {
	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}

	lInfo, err := os.Stat(lhs)
	must(err)
	rInfo, err := os.Stat(rhs)
	must(err)

	// Both either files or directories.
	if lInfo.IsDir() != rInfo.IsDir() {
		return false
	}

	// Both files?
	if !lInfo.IsDir() {
		lBuf, err := ioutil.ReadFile(lhs)
		must(err)
		rBuf, err := ioutil.ReadFile(rhs)
		must(err)
		return bytes.Equal(lBuf, rBuf)
	}

	// Dirs.
	lList, err := ioutil.ReadDir(lhs)
	must(err)
	rList, err := ioutil.ReadDir(rhs)
	must(err)

	if len(lList) != len(rList) {
		return false
	}

	for i := range lList {
		ok := pathsMatch(
			filepath.Join(lhs, lList[i].Name()),
			filepath.Join(rhs, lList[i].Name()))
		if !ok {
			return false
		}
	}

	return true
}
