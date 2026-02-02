// Copyright 2016 - 2024 The excelize Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
//
// <PATCHED LINE> prefetchReader: in-memory buffer for the columns/token flow
// to reduce repeated small reads from disk. Refill is synchronous only (no
// background goroutine) to avoid scheduler wakeup overhead that dominates
// CPU profiles when using async prefetch.
//
// Execution flow:
//
//  1. Consumer (e.g. xml.Decoder.Token()) calls Read().
//  2. Read() serves bytes from the in-memory buffer (dataBuffer[readPos:writePos]).
//  3. When the buffer is empty, Read() refills synchronously from the source
//     (one larger read), then serves from the buffer again.
//
// No channels, no worker goroutine, no notewakeup/semawakeup: a single
// goroutine (the caller) does all work. Buffering still reduces the number
// of actual file reads by doing fewer, larger reads into the buffer.

package excelize

import (
	"io"
	"sync"
)

const (
	// defaultPrefetchBufferSize is the total capacity of the in-memory buffer.
	// Token/Columns reads are satisfied from this buffer instead of many small file reads.
	defaultPrefetchBufferSize = 1024 * 1024
	// defaultPrefetchRefillThreshold is unused when async prefetch is disabled;
	// kept for API compatibility with newPrefetchReader(bufferSize, refillThreshold).
	defaultPrefetchRefillThreshold = 100 * 1024
)

// prefetchReader wraps an io.Reader and maintains an in-memory buffer. Read()
// serves data from the buffer; when the buffer is empty it refills synchronously
// from the source. No background goroutine is used, avoiding scheduler overhead.
type prefetchReader struct {
	sourceReader io.Reader  // underlying reader (e.g. *os.File from readTemp)
	dataBuffer   []byte     // in-memory buffer; valid data is dataBuffer[readPos:writePos]
	readPos      int        // next byte to return to the consumer
	writePos     int        // first byte past valid data in dataBuffer
	bufferMu     sync.Mutex // protects buffer state (concurrent Read is allowed by io.Reader)
	lastError    error      // first I/O or EOF error; once set, Read() returns it
}

// newPrefetchReader returns a reader that buffers up to bufferSize bytes and
// refills synchronously when the buffer is empty. If bufferSize or refillThreshold
// is <= 0, the corresponding default is used. refillThreshold is ignored (no async prefetch).
func newPrefetchReader(src io.Reader, bufferSize, refillThreshold int) *prefetchReader {
	if bufferSize <= 0 {
		bufferSize = defaultPrefetchBufferSize
	}
	if refillThreshold <= 0 {
		refillThreshold = defaultPrefetchRefillThreshold
	}
	_ = refillThreshold // unused when sync-only
	return &prefetchReader{
		sourceReader: src,
		dataBuffer:   make([]byte, 0, bufferSize),
	}
}

// Read implements io.Reader. It serves data from the in-memory buffer; when the
// buffer is empty it refills synchronously from the source, then serves again.
// No background goroutine is used.
func (p *prefetchReader) Read(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}

	p.bufferMu.Lock()
	if p.lastError != nil {
		err = p.lastError
		p.bufferMu.Unlock()
		return 0, err
	}
	if p.readPos < p.writePos {
		n = copy(b, p.dataBuffer[p.readPos:p.writePos])
		p.readPos += n
		p.bufferMu.Unlock()
		return n, nil
	}
	p.bufferMu.Unlock()

	p.refillBufferFromSource()

	p.bufferMu.Lock()
	if p.lastError != nil {
		err = p.lastError
		p.bufferMu.Unlock()
		return 0, err
	}
	if p.readPos >= p.writePos {
		p.bufferMu.Unlock()
		// Buffer still empty; retry once (e.g. source returned 0,nil).
		p.refillBufferFromSource()
		p.bufferMu.Lock()
		if p.lastError != nil {
			err = p.lastError
			p.bufferMu.Unlock()
			return 0, err
		}
		if p.readPos >= p.writePos {
			p.bufferMu.Unlock()
			return 0, nil
		}
	}
	n = copy(b, p.dataBuffer[p.readPos:p.writePos])
	p.readPos += n
	p.bufferMu.Unlock()
	return n, nil
}

// refillBufferFromSource compacts the buffer (if needed), reads from the source
// into the buffer, and updates writePos and lastError. No lock is held during I/O.
func (p *prefetchReader) refillBufferFromSource() {
	p.bufferMu.Lock()
	if p.readPos > 0 {
		p.writePos = copy(p.dataBuffer, p.dataBuffer[p.readPos:p.writePos])
		p.readPos = 0
	}
	capacityLeft := cap(p.dataBuffer) - p.writePos
	p.bufferMu.Unlock()

	if capacityLeft <= 0 {
		return
	}

	readBuf := make([]byte, capacityLeft)
	bytesRead, readErr := p.sourceReader.Read(readBuf)

	p.bufferMu.Lock()
	defer p.bufferMu.Unlock()

	if readErr != nil && readErr != io.EOF {
		p.lastError = readErr
		return
	}

	if bytesRead > 0 {
		p.dataBuffer = append(p.dataBuffer[:p.writePos], readBuf[:bytesRead]...)
		p.writePos += bytesRead
	}

	if readErr == io.EOF {
		p.lastError = io.EOF
	}
}
