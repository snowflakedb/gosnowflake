package os

import (
	"runtime"
	"strings"
	"testing"
)

func TestParseProcMapsGlibc(t *testing.T) {
	maps := `7f1234560000-7f1234580000 r-xp 00000000 08:01 12345  /usr/lib/x86_64-linux-gnu/libc.so.6
7f1234580000-7f1234590000 r--p 00020000 08:01 12345  /usr/lib/x86_64-linux-gnu/libc.so.6`
	family, path := parseProcMapsForLibc(strings.NewReader(maps))
	if family != "glibc" {
		t.Errorf("expected glibc, got %q", family)
	}
	if path != "/usr/lib/x86_64-linux-gnu/libc.so.6" {
		t.Errorf("unexpected path: %q", path)
	}
}

func TestParseProcMapsMusl(t *testing.T) {
	maps := `7f1234560000-7f1234580000 r-xp 00000000 08:01 12345  /lib/ld-musl-x86_64.so.1`
	family, path := parseProcMapsForLibc(strings.NewReader(maps))
	if family != "musl" {
		t.Errorf("expected musl, got %q", family)
	}
	if path != "/lib/ld-musl-x86_64.so.1" {
		t.Errorf("unexpected path: %q", path)
	}
}

func TestParseProcMapsMuslLibc(t *testing.T) {
	maps := `7f1234560000-7f1234580000 r-xp 00000000 08:01 12345  /lib/libc.musl-x86_64.so.1`
	family, path := parseProcMapsForLibc(strings.NewReader(maps))
	if family != "musl" {
		t.Errorf("expected musl, got %q", family)
	}
	if path != "/lib/libc.musl-x86_64.so.1" {
		t.Errorf("unexpected path: %q", path)
	}
}

func TestParseProcMapsEmpty(t *testing.T) {
	family, path := parseProcMapsForLibc(strings.NewReader(""))
	if family != "" || path != "" {
		t.Errorf("expected empty, got family=%q path=%q", family, path)
	}
}

func TestParseProcMapsNoLibc(t *testing.T) {
	maps := `7f1234560000-7f1234580000 r-xp 00000000 08:01 12345  /usr/lib/libpthread.so.0
7fff12340000-7fff12360000 rw-p 00000000 00:00 0  [stack]`
	family, path := parseProcMapsForLibc(strings.NewReader(maps))
	if family != "" || path != "" {
		t.Errorf("expected empty, got family=%q path=%q", family, path)
	}
}

func TestParseProcMapsShortLines(t *testing.T) {
	maps := `7f1234560000-7f1234580000 r-xp 00000000 08:01 12345
7fff12340000-7fff12360000 rw-p 00000000 00:00 0`
	family, path := parseProcMapsForLibc(strings.NewReader(maps))
	if family != "" || path != "" {
		t.Errorf("expected empty for short lines, got family=%q path=%q", family, path)
	}
}

func TestCompareVersions(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"2.31", "2.17", 1},
		{"2.17", "2.31", -1},
		{"2.31", "2.31", 0},
		{"2.31.1", "2.31", 1},
		{"2.31", "2.31.1", -1},
		{"1.2.3", "1.2.3", 0},
		{"10.0", "9.99", 1},
	}
	for _, c := range cases {
		got := compareVersions(c.a, c.b)
		if got != c.want {
			t.Errorf("compareVersions(%q, %q) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestGetLibcInfoNonLinux(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("this test is for non-Linux platforms")
	}
	info := GetLibcInfo()
	if info.Family != "" || info.Version != "" {
		t.Errorf("expected empty LibcInfo on non-Linux, got %+v", info)
	}
}
