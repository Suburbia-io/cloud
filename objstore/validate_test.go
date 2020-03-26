package objstore

import "testing"

func TestValidateDay(t *testing.T) {
	type TestCase struct {
		In  string
		Err error
	}

	cases := []TestCase{
		{"2006-01-01", nil},
		{"2006-01-02", nil},
		{"2006-01-03", nil},
		{"2006-01-30", nil},
		{"2006-01-31", nil},
		{"2006-01-32", ErrInvalidDay},
		{"2020-12-01", nil},
		{"2020-12-31", nil},
		{"2020-12-32", ErrInvalidDay},
		{"000-01-01", ErrInvalidDay},
		{"100-01-01", ErrInvalidDay},
		{"2020-1-01", ErrInvalidDay},
		{"2020-01-1", ErrInvalidDay},
		{"0000-00-00", ErrInvalidDay},
	}

	for _, tc := range cases {
		err := validateDay(tc.In)
		if err != tc.Err {
			t.Fatalf("%s: %v != %v", tc.In, err, tc.Err)
		}
	}
}

func TestValidateVersion(t *testing.T) {
	type TestCase struct {
		In  string
		Err error
	}

	cases := []TestCase{
		{"2006-01-01.00", nil},
		{"2006-01-02.00", nil},
		{"2006-01-03.22", nil},
		{"2006-01-30.13", nil},
		{"2006-01-31.99", nil},
		{"2006-01-32.0", ErrInvalidVersion},
		{"2020-12-01.10", nil},
		{"2020-12-31.11", nil},
		{"2020-12-32.10", ErrInvalidVersion},
		{"000-01-01.00", ErrInvalidVersion},
		{"100-01-01.00", ErrInvalidVersion},
		{"2020-1-01.00", ErrInvalidVersion},
		{"2020-01-1.00", ErrInvalidVersion},
		{"0000-00-00.00", ErrInvalidVersion},
		{"2020-12-10.xx", ErrInvalidVersion},
		{"2020-12-10", ErrInvalidVersion},
	}

	for _, tc := range cases {
		err := validateVersion(tc.In)
		if err != tc.Err {
			t.Fatalf("%s: %v != %v", tc.In, err, tc.Err)
		}
	}
}

func TestValidateCPGEUVendor(t *testing.T) {
	type TestCase struct {
		In  string
		Err error
	}

	cases := []TestCase{
		{"chicken", nil},
		{"dingo", nil},
		{"goat", nil},
		{"toad", nil},
		{"GOAT", ErrInvalidVendor},
		{"Goat", ErrInvalidVendor},
		{"", ErrInvalidVendor},
		{"xx", ErrInvalidVendor},
	}

	for _, tc := range cases {
		err := validateCPGEUVendor(tc.In)
		if err != tc.Err {
			t.Fatalf("%s: %v != %v", tc.In, err, tc.Err)
		}
	}
}
