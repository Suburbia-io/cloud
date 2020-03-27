package batch

type Task struct {
	Executable string            // Full path to executable.
	Env        map[string]string // Environment variables and values.
	Args       string            // Arguments to the executable.
}

type Result struct {
	Code   int    // Executable exit code. 0 is traditionally success.
	Output string // Combined command output.
}
