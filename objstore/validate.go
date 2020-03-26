package objstore

import (
	"time"
)

func validateDay(day string) error {
	d, err := time.Parse("2006-01-02", day)
	if err != nil {
		return ErrInvalidDay
	}
	if d.Format("2006-01-02") != day {
		return ErrInvalidDay
	}
	return nil
}

func validateVersion(version string) error {
	if len(version) != 13 {
		return ErrInvalidVersion
	}
	if err := validateDay(version[:10]); err != nil {
		return ErrInvalidVersion
	}

	ext := version[10:]
	if ext[0] != '.' {
		return ErrInvalidVersion
	}
	for _, c := range ext[1:] {
		switch c {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			// OK.
		default:
			return ErrInvalidVersion
		}
	}
	return nil
}

func validateCPGEUVendor(vendor string) error {
	switch vendor {
	case "chicken", "dingo", "goat", "toad":
		return nil
	default:
		return ErrInvalidVendor
	}
}
