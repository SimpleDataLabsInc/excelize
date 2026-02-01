// Copyright 2016 - 2024 The excelize Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
//
// <PATCHED LINE> prefetchReader: in-memory buffer with async disk prefetch
// for the columns/token flow to reduce repeated file reads and latency.

package excelize

import (
	"io"
	"sync"
)

const (
	// defaultPrefetchBufferSize is the size of the in-memory buffer (10 KB).
	// Token/Columns reads are served from this buffer instead of hitting disk.
	defaultPrefetchBufferSize = 10 * 1024
	// defaultPrefetchRefillThreshold: when remaining bytes in buffer fall
	// below this (1 KB), a background prefetch from disk is triggered.
	defaultPrefetchRefillThreshold = 1 * 1024
)

// prefetchReader wraps an io.Reader and keeps a buffer in memory. When the
// buffer has fewer than refillThreshold bytes left, it prefetches more data
// from the underlying reader (in a goroutine) so that subsequent Read() calls
// are served from memory instead of disk. This reduces latency when the
// consumer (e.g. xml.Decoder.Token()) performs many small reads.
type prefetchReader struct {
	src             io.Reader
	buf             []byte
	r, w            int
	refillThreshold int
	mu              sync.Mutex
	srcMu           sync.Mutex // serializes reads from src (file must not be read concurrently)
	prefetching     bool
	err             error
}

// newPrefetchReader returns a reader that buffers up to bufferSize bytes and
// triggers async refill when remaining data drops below refillThreshold.
// If bufferSize or refillThreshold is <= 0, defaults are used.
func newPrefetchReader(src io.Reader, bufferSize, refillThreshold int) *prefetchReader {
	if bufferSize <= 0 {
		bufferSize = defaultPrefetchBufferSize
	}
	if refillThreshold <= 0 {
		refillThreshold = defaultPrefetchRefillThreshold
	}
	return &prefetchReader{
		src:             src,
		buf:             make([]byte, 0, bufferSize),
		refillThreshold: refillThreshold,
	}
}

// Read implements io.Reader. Data is served from the in-memory buffer; when
// the buffer is low, a background prefetch is started. When the buffer is
// empty, a synchronous refill is performed so the caller never blocks
// indefinitely.
func (p *prefetchReader) Read(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.err != nil {
		return 0, p.err
	}

	// Serve from buffer if we have data.
	if p.r < p.w {
		n = copy(b, p.buf[p.r:p.w])
		p.r += n
		// Trigger async prefetch when remaining data is below threshold.
		if (p.w-p.r) < p.refillThreshold && !p.prefetching {
			p.startRefillLocked()
		}
		return n, nil
	}

	// Buffer empty: refill synchronously so caller gets data (or EOF).
	p.refillLocked()
	if p.err != nil {
		return 0, p.err
	}
	if p.r >= p.w {
		return 0, io.EOF
	}
	n = copy(b, p.buf[p.r:p.w])
	p.r += n
	if (p.w-p.r) < p.refillThreshold && !p.prefetching {
		p.startRefillLocked()
	}
	return n, nil
}

// refillLocked compacts the buffer and reads from src into it. Caller must
// hold p.mu. Does not set p.err on EOF (so Read can return 0, io.EOF).
func (p *prefetchReader) refillLocked() {
	// Compact: move unread data to the start.
	if p.r > 0 {
		p.w = copy(p.buf, p.buf[p.r:p.w])
		p.r = 0
	}
	capLeft := cap(p.buf) - p.w
	if capLeft <= 0 {
		return
	}
	if p.w+capLeft > len(p.buf) {
		p.buf = p.buf[:p.w+capLeft]
	}
	p.srcMu.Lock()
	nn, err := p.src.Read(p.buf[p.w : p.w+capLeft])
	p.srcMu.Unlock()
	p.w += nn
	if err != nil && err != io.EOF {
		p.err = err
		return
	}
	if err == io.EOF && nn == 0 {
		p.err = io.EOF
	}
}

// startRefillLocked starts a goroutine to refill the buffer. Caller must hold
// p.mu and must have set p.prefetching = true before returning (so we don't
// start multiple prefetches). We set prefetching inside the goroutine after
// releasing the lock for the actual I/O, so we need to set it before spawning.
func (p *prefetchReader) startRefillLocked() {
	if p.prefetching || p.err != nil {
		return
	}
	p.prefetching = true
	go p.prefetch()
}

func (p *prefetchReader) prefetch() {
	// Compact and decide how much to read (holding lock only briefly for
	// buffer state, then release for I/O).
	p.mu.Lock()
	if p.r > 0 {
		p.w = copy(p.buf, p.buf[p.r:p.w])
		p.r = 0
	}
	capLeft := cap(p.buf) - p.w
	p.mu.Unlock()

	if capLeft <= 0 {
		p.mu.Lock()
		p.prefetching = false
		p.mu.Unlock()
		return
	}

	readBuf := make([]byte, capLeft)
	p.srcMu.Lock()
	nn, err := p.src.Read(readBuf)
	p.srcMu.Unlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	p.prefetching = false
	if p.err != nil {
		return
	}
	if err != nil && err != io.EOF {
		p.err = err
		return
	}
	if nn > 0 {
		if p.w+nn > cap(p.buf) {
			p.buf = append(p.buf[:p.w], readBuf[:nn]...)
		} else {
			p.buf = p.buf[:p.w+nn]
			copy(p.buf[p.w:], readBuf[:nn])
		}
		p.w += nn
	}
	// Only set EOF when source is done and we have no unread data to deliver.
	if err == io.EOF && p.r >= p.w {
		p.err = io.EOF
	}
}
