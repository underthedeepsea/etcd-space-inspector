//go:build !darwin && !dragonfly && !freebsd && !linux && !openbsd && !windows
// +build !darwin,!dragonfly,!freebsd,!linux,!openbsd,!windows

package task

func diskFreeBytes(string) uint64 { return 0 }
