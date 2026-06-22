//go:build android

package main

// #cgo LDFLAGS: -llog
// #include <android/log.h>
// #include <stdlib.h>
import "C"

import (
	"bytes"
	"log"
	"strings"
	"sync"
	"unsafe"
)

type androidWriter struct {
	mu   sync.Mutex
	buf  bytes.Buffer
	tag  string
	prio C.int
}

func (w *androidWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(p)
	w.buf.Write(p)
	for {
		line, err := w.buf.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			continue
		}
		ctag := C.CString(w.tag)
		cmsg := C.CString(line)
		C.__android_log_write(w.prio, ctag, cmsg)
		C.free(unsafe.Pointer(ctag))
		C.free(unsafe.Pointer(cmsg))
	}
	return n, nil
}

func init() {
	log.SetOutput(&androidWriter{tag: "huginn", prio: C.ANDROID_LOG_INFO})
}
