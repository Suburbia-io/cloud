package objstore

import "strings"

func Join(parts ...string) string {
	return strings.Join(parts, "/")
}
