package test

import (
	"fmt"
	"sync"
	"testing"
	"wangweizZZ/kv/pkg/bitcask"
)

// go test -bench=. -benchmem -cpuprofile profile.out
// pprof -http=:8080 profile.out

var (
	db   *bitcask.Bitcask
	once sync.Once
)

func BenchmarkKv(b *testing.B) {
	once.Do(func() {
		dataDb, err := bitcask.Open("./data")
		if err != nil {
			b.Error(err.Error())
		}
		db = dataDb
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Put(fmt.Sprintf("key_%d", i), fmt.Sprintf("data_%d", i))
	}
}
