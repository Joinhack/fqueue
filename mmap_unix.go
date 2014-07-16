// +build darwin freebsd linux netbsd openbsd

package fqueue

import (
	"syscall"
	"unsafe"
)

const (
	PageSize = 4096
	MetaSize = 4096
)

func mmap(fd uintptr, off int64, l, inprot int) ([]byte, error) {
	prot := syscall.PROT_READ
	switch {
	case inprot&WRITE != 0:
		prot = syscall.PROT_WRITE
	case inprot&RDWR != 0:
		prot |= syscall.PROT_WRITE
	}
	b, err := syscall.Mmap(int(fd), off, l, prot, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func msync(addr, len uintptr) error {
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, addr, len, syscall.MS_SYNC)
	if errno != 0 {
		return syscall.Errno(errno)
	}
	return nil
}

func unmap(p []byte) error {
	_, _, errno := syscall.Syscall(syscall.SYS_MUNMAP, uintptr(unsafe.Pointer(&p[0])), uintptr(len(p)), 0)
	if errno != 0 {
		return syscall.Errno(errno)
	}
	return nil
}
