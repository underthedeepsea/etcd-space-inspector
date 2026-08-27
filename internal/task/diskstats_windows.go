//go:build windows
// +build windows

package task

import (
	"sync"
	"syscall"
	"unsafe"
)

var (
	diskFreeProcOnce sync.Once
	diskFreeProc     *syscall.Proc
)

func diskFreeBytes(path string) uint64 {
	diskFreeProcOnce.Do(func() {
		dll, err := syscall.LoadDLL("kernel32.dll")
		if err != nil {
			return
		}
		diskFreeProc, _ = dll.FindProc("GetDiskFreeSpaceExW")
	})
	if diskFreeProc == nil {
		return 0
	}
	pathPointer, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0
	}
	var available uint64
	result, _, _ := diskFreeProc.Call(
		uintptr(unsafe.Pointer(pathPointer)),
		uintptr(unsafe.Pointer(&available)),
		0,
		0,
	)
	if result == 0 {
		return 0
	}
	return available
}
