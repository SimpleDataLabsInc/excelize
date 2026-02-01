package excelize

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestPrefetchReader(t *testing.T) {
	// Small source: no prefetch needed, just buffering.
	src := strings.NewReader("hello world")
	r := newPrefetchReader(src, 64, 4)
	buf := make([]byte, 100)
	n, err := r.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 11 || string(buf[:n]) != "hello world" {
		t.Fatalf("got %d %q", n, buf[:n])
	}
	n, err = r.Read(buf)
	if n != 0 || err != io.EOF {
		t.Fatalf("expected EOF, got n=%d err=%v", n, err)
	}
}

func TestPrefetchReader_LargeSource(t *testing.T) {
	// Source larger than buffer: exercises refill and prefetch path.
	data := bytes.Repeat([]byte("x"), 20*1024)
	src := bytes.NewReader(data)
	r := newPrefetchReader(src, 1024, 256)
	var out bytes.Buffer
	_, err := io.Copy(&out, r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), data) {
		t.Fatalf("lengths: got %d want %d", out.Len(), len(data))
	}
}

func TestPrefetchReader_ReadByteByByte(t *testing.T) {
	// Simulates many small reads like xml.Decoder.Token().
	data := bytes.Repeat([]byte("a"), 2048)
	src := bytes.NewReader(data)
	r := newPrefetchReader(src, 512, 64)
	for i := 0; i < len(data); i++ {
		b := make([]byte, 1)
		n, err := r.Read(b)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if n != 1 || b[0] != 'a' {
			t.Fatalf("at %d: n=%d b=%q", i, n, b)
		}
	}
	n, _ := r.Read(make([]byte, 1))
	if n != 0 {
		t.Fatalf("expected 0 after exhaustion, got %d", n)
	}
}
