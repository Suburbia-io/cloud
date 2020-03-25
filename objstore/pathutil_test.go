package objstore

import "testing"

func TestBase(t *testing.T) {
	type TestCase struct {
		In, Out string
	}

	cases := []TestCase{
		{
			In:  "",
			Out: "",
		}, {
			In:  "/",
			Out: "",
		}, {
			In:  "xyz",
			Out: "xyz",
		}, {
			In:  "abcd/abc",
			Out: "abc",
		},
	}

	for _, tc := range cases {
		out := Base(tc.In)
		if out != tc.Out {
			t.Fatalf("%v != %v", out, tc.Out)
		}
	}
}

func TestDir(t *testing.T) {
	type TestCase struct {
		In, Out string
	}

	cases := []TestCase{
		{
			In:  "",
			Out: "",
		}, {
			In:  "/",
			Out: "",
		}, {
			In:  "xyz",
			Out: "",
		}, {
			In:  "abcd/abc",
			Out: "abcd",
		}, {
			In:  "abcd/abc/",
			Out: "abcd",
		},
	}

	for _, tc := range cases {
		out := Dir(tc.In)
		if out != tc.Out {
			t.Fatalf("%v != %v", out, tc.Out)
		}
	}
}
