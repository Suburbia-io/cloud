package objstore

import "regexp"

func validateVersion(version string) {
	rx := regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}\.[0-9]{2}$`)
	if !rx.MatchString(version) {
		panic("Invalid version: " + version)
	}
}

func validateDay(day string) {
	rx := regexp.MustCompile(`^[1-9]{4}-[0-9]{2}-[0-9]{2}$`)
	if !rx.MatchString(day) {
		panic("Invalid day: " + day)
	}
}
