package bridge

import "testing"

func TestGetBufCap(t *testing.T) {
	cases := []struct {
		n       int
		wantCap int
	}{
		{1, 64},
		{63, 64},
		{64, 64},
		{65, 128},
		{1000, 1024},
		{10240, 16384},
		{1 << 20, 1 << 20},
	}
	for _, tc := range cases {
		b := getBuf(tc.n)
		if len(b) != tc.n {
			t.Errorf("getBuf(%d): len = %d, want %d", tc.n, len(b), tc.n)
		}
		if cap(b) != tc.wantCap {
			t.Errorf("getBuf(%d): cap = %d, want %d", tc.n, cap(b), tc.wantCap)
		}
	}
}

func TestGetBufUnpooled(t *testing.T) {
	n := (1 << 20) + 1
	b := getBuf(n)
	if len(b) != n {
		t.Errorf("getBuf(%d): len = %d, want %d", n, len(b), n)
	}
	if cap(b) < n {
		t.Errorf("getBuf(%d): cap = %d, want >= %d", n, cap(b), n)
	}
}

func TestPutGetSameClass(t *testing.T) {
	// sync.Pool reuse is not guaranteed under GC, so we assert class stability
	// (matching capacity) rather than pointer identity.
	b := getBuf(1000)
	for i := range b {
		b[i] = byte(i)
	}
	putBuf(b)
	got := getBuf(1000)
	if cap(got) != 1024 {
		t.Errorf("cap after put/get = %d, want 1024", cap(got))
	}
	if len(got) != 1000 {
		t.Errorf("len after put/get = %d, want 1000", len(got))
	}
}

func TestPutBufNonClassIsNoop(t *testing.T) {
	// Non-power-of-two and out-of-range capacities must be dropped silently.
	putBuf(make([]byte, 0, 100))   // not a power of two
	putBuf(make([]byte, 0, 32))    // below min class
	putBuf(make([]byte, 0))        // zero cap
	putBuf(make([]byte, 0, 1<<21)) // above max class
}

func TestGetBufNonPositive(t *testing.T) {
	if got := getBuf(0); len(got) != 0 {
		t.Errorf("getBuf(0): len = %d, want 0", len(got))
	}
	if got := getBuf(-5); len(got) != 0 {
		t.Errorf("getBuf(-5): len = %d, want 0", len(got))
	}
}
