package objstore

var CPGEU = CPGEUPaths{}

type CPGEUPaths struct{}

func (p CPGEUPaths) Bucket() string {
	return "cpg-eu"
}

// Stage0Prefix: The prefix under which all stage0 files are stored.
func (p CPGEUPaths) Stage0Prefix() string {
	return "stage0"
}

// Stage0VendorPrefix: The prefix under which all dump versions are stored.
func (p CPGEUPaths) Stage0VendorPrefix(vendor string) string {
	p.validateVendor(vendor)
	return Join("stage0", vendor)
}

// Stage0DumpTarGZ: A zipped dump directory. The version should have the form
// YYYY-MM-DD.NN.
func (p CPGEUPaths) Stage0DumpTarGZ(vendor, version string) string {
	p.validateVendor(vendor)
	validateVersion(version)
	return Join("stage0", vendor, version+".tar.gz")
}

// Stage0SplitVendorPrefix: The prefix under which all days split from the
// given dump are stored. The files under this prefix should all be of the form
// YYYY-MM-DD.csv.gz.
func (p CPGEUPaths) Stage0SplitDumpPrefix(vendor, version string) string {
	p.validateVendor(vendor)
	validateVersion(version)
	return Join("stage0.split", vendor, version)
}

// Stage0SplitDumpDaily: A single dailiy dump file.
func (p CPGEUPaths) Stage0SplitDumpDaily(vendor, version, day string) string {
	p.validateVendor(vendor)
	validateVersion(version)
	validateDay(day)
	return Join("stage0.split", vendor, version, day+".csv.gz")
}

// Stage1Prefix: The prefix under which stage1 tables are stored. Each stage1
// table has the form YYYY-MM-DD.tar.gz.
func (p CPGEUPaths) Stage1Prefix(version string) string {
	return Join("build", version, "stage1")
}

// Stage1DumpTarGZ: A zipped stage1 daily table. The `day` parameter should
// have the form YYYY-MM-DD.
func (p CPGEUPaths) Stage1DumpTarGZ(version, day string) string {
	validateDay(day)
	return Join("build", version, "stage1", day+".tar.gz")
}

// Stage2Prefix: The prefix under which stage2 tables are stored. Each stage2
// table has the form YYYY-MM-DD.tar.gz.
func (p CPGEUPaths) Stage2Prefix(version string) string {
	return Join("build", version, "stage2")
}

// Stage2DumpTarGZ: A zipped stage2 daily table. The `day` parameter should
// have the form YYYY-MM-DD.
func (p CPGEUPaths) Stage2DumpTarGZ(version, day string) string {
	validateDay(day)
	return Join("build", version, "stage2", day+".tar.gz")
}

// Stage3Prefix: The prefix under which stage3 daily zipped CSV files are
// stored. Stage3 files belong in their own bucket.
func (p CPGEUPaths) Stage3Prefix(version string) string {
	validateVersion(version)
	return Join("cpg-eu", version)
}

// Stage3Daily: Full path to a daily release version.
func (p CPGEUPaths) Stage3Daily(version, day string) string {
	validateDay(day)
	return Join("cpg-eu", version, day+".csv.gz")
}

// ----------------------------------------------------------------------------

func (p CPGEUPaths) validateVendor(vendor string) {
	switch vendor {
	case "chicken", "dingo", "goat", "toad":
		return
	default:
		panic("Invalid vendor: " + vendor)
	}
}
