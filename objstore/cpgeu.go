package objstore

var CPGEU = CPGEUPaths{}

type CPGEUPaths struct{}

// Stage0Prefix: The prefix under which all stage0 files are stored.
func (CPGEUPaths) Stage0Prefix() string {
	return "stage0"
}

// Stage0VendorPrefix: The prefix under which all dump versions are stored.
func (CPGEUPaths) Stage0VendorPrefix(vendor string) string {
	return Join("stage0", vendor)
}

// Stage0DumpTarGZ: A zipped dump directory. The version should have the form
// YYYY-MM-DD.NN.
func (CPGEUPaths) Stage0DumpTarGZ(vendor, version string) string {
	return Join("stage0", vendor, version+".tar.gz")
}

// Stage0SplitVendorPrefix: The prefix under which all days split from the
// given dump are stored. The files under this prefix should all be of the form
// YYYY-MM-DD.csv.gz.
func (CPGEUPaths) Stage0SplitDumpPrefix(vendor, version string) string {
	return Join("stage0", "_SPLIT_", vendor, version)
}
