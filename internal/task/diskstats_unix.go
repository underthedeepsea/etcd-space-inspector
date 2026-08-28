//go:build darwin || dragonfly || freebsd || linux || openbsd
// +build darwin dragonfly freebsd linux openbsd

package task

import "syscall"

func diskFreeBytes(path string) uint64 {
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs(path, &filesystem); err != nil {
		return 0
	}
	return filesystem.Bavail * uint64(filesystem.Bsize)
}
