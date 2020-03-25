package objstore

import "regexp"

func validateVersion(version string) error {
	rx := regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}\.[0-9]{2}$`)
	if !rx.MatchString(version) {
		return ErrInvalidVersion
	}
	return nil
}

func validateDay(day string) error {
	rx := regexp.MustCompile(`^[1-9]{4}-[0-9]{2}-[0-9]{2}$`)
	if !rx.MatchString(day) {
		return ErrInvalidDay
	}
	return nil
}
