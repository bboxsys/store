//go:build windows

package store

// getDiskUsagePlatform is a no-op on Windows.
func getDiskUsagePlatform(path string) (total uint64, free uint64, usedPct float64) {
	return 0, 0, 0
}
