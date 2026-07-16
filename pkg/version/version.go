package version

// These variables are overridden at build time via -ldflags.
var (
	Version   = "dev"
	GitCommit = "unknown"
	BuildDate = "unknown"
)

// String returns a human-readable version string.
func String() string {
	return Version + " (commit=" + GitCommit + ", built=" + BuildDate + ")"
}
