package fqueue

import (
	"errors"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

var handleLock sync.Mutex
var handleMap = map[uintptr]syscall.Handle{}

func mmap(fd uintptr, off int64, l, inprot int) (sli []byte, err error) {
	flProtect := uint32(syscall.PAGE_READONLY)
	dwDesiredAccess := uint32(syscall.FILE_MAP_READ)
	switch {
	case inprot&WRITE != 0:
		flProtect = syscall.PAGE_READWRITE
		dwDesiredAccess = syscall.FILE_MAP_WRITE
	case inprot&RDWR != 0:
		flProtect = syscall.PAGE_READWRITE
		dwDesiredAccess = syscall.FILE_MAP_WRITE
	}
	h, errno := syscall.CreateFileMapping(syscall.Handle(fd), nil, flProtect, 0, uint32(l), nil)
	if h == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}
	addr, errno := syscall.MapViewOfFile(h, dwDesiredAccess, uint32(off>>32), uint32(off&0xFFFFFFFF), uintptr(l))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}
	handleLock.Lock()
	handleMap[addr] = h
	handleLock.Unlock()
	var header = struct {
		d    uintptr
		l, c int
	}{addr, l, l}
	sli = *(*[]byte)(unsafe.Pointer(&header))
	println(11)
	return sli, nil
}

func unmap(p []byte) error {
	addr := uintptr(unsafe.Pointer(&p[0]))
	syscall.FlushViewOfFile(addr, uintptr(len(p)))
	err := syscall.UnmapViewOfFile(addr)
	if err != nil {
		return err
	}

	handleLock.Lock()
	defer handleLock.Unlock()
	handle, ok := handleMap[addr]
	if !ok {
		// should be impossible; we would've errored above
		return errors.New("unknown base address")
	}
	delete(handleMap, addr)

	e := syscall.CloseHandle(syscall.Handle(handle))
	return os.NewSyscallError("CloseHandle", e)
}
