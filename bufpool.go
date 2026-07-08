package bridge

import (
	"math/bits"
	"sync"
)

const (
	minClassBits = 6  // smallest pooled class: 64B
	maxClassBits = 20 // largest pooled class: 1MB
)

// bufPools holds one sync.Pool per power-of-two size class. Only indices
// [minClassBits, maxClassBits] are populated. Pointers to slices are stored to
// avoid the interface-boxing allocation on Put.
var bufPools [maxClassBits + 1]sync.Pool

func init() {
	for i := minClassBits; i <= maxClassBits; i++ {
		size := 1 << i
		bufPools[i].New = func() any {
			b := make([]byte, size)
			return &b
		}
	}
}

// classBits returns the size-class exponent for n bytes: the smallest power of
// two >= n, clamped up to the 64B minimum.
func classBits(n int) int {
	exp := bits.Len(uint(n - 1))
	if exp < minClassBits {
		exp = minClassBits
	}
	return exp
}

// getBuf returns a slice with len == n and cap equal to the smallest pooled
// power of two >= n (min 64B).
func getBuf(n int) []byte {
	if n <= 0 {
		return make([]byte, 0)
	}
	exp := classBits(n)
	// ponytail: buffers larger than 1MB bypass the pool entirely (unpooled).
	// Upgrade path: add larger classes or a slab allocator if these get hot.
	if exp > maxClassBits {
		return make([]byte, n)
	}
	bp := bufPools[exp].Get().(*[]byte)
	return (*bp)[:n]
}

// putBuf returns b to the bucket matching its capacity. Capacities that are not
// an exact pooled power-of-two class are dropped.
func putBuf(b []byte) {
	c := cap(b)
	if c == 0 || c&(c-1) != 0 {
		return
	}
	exp := bits.TrailingZeros(uint(c))
	if exp < minClassBits || exp > maxClassBits {
		return
	}
	full := b[:c]
	bufPools[exp].Put(&full)
}
