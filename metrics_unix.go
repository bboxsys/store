//go:build !windows

package store

import "syscall"

// getDiskUsagePlatform returns disk total, free, and used percentage using syscall.Statfs.
func getDiskUsagePlatform(path string) (total uint64, free uint64, usedPct float64) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, 0, 0
	}
	total = stat.Blocks * uint64(stat.Bsize)
	free = stat.Bavail * uint64(stat.Bsize)
	if total > 0 {
		used := total - free
		usedPct = float64(used) / float64(total) * 100.0
	}
	return total, free, usedPct
}
