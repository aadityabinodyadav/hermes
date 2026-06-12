package os_fundamentals

import "testing"

func BenchmarkStringConcat(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = "hello" + "world"
	}
}
