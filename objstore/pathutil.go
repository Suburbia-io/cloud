package objstore

import "strings"

func Join(parts ...string) string {
	return strings.Join(parts, "/")
}

func Base(path string) string {
	idx := strings.LastIndex(path, "/")
	if idx == -1 {
		return path
	}
	return path[idx+1:]
}

func Dir(path string) string {
	idx := strings.LastIndex(path, "/")
	if idx == -1 {
		return ""
	}
	return path[:idx]
}
