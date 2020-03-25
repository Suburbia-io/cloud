package objstore

var Release = ReleasePaths{}

type ReleasePaths struct{}

func (p ReleasePaths) Bucket() string {
	return "release"
}

func (p ReleasePaths) CPGEUPrefix(version string) string {
	return Join("cpg-eu", version)
}

func (p ReleasePaths) CPGEUDaily(version, day string) string {
	validateVersion(version)
	validateDay(day)
	return Join("cpg-eu", version, day+".csv.gz")
}
