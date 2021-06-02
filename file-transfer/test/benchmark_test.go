package filetransfer

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

const filePath string = "./main.go"

func Test_aa(t *testing.T) {
	by, _ := ioutil.ReadFile(filePath)
	ll := len(by)
	fmt.Println(ll)

	file, _ := os.Open(filePath)
	f, _ := file.Stat()
	fmt.Println(f.Size())

	file2, _ := os.Open(filePath)
	indx, _ := file2.Seek(0, 2)
	fmt.Println(indx)
}

func Benchmark_A(b *testing.B) {

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		by, _ := ioutil.ReadFile(filePath)
		_ = len(by)
	}
}

func Benchmark_B(b *testing.B) {

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		file, _ := os.Open(filePath)
		file.Seek(0, 2)
	}
}
func Benchmark_C(b *testing.B) {

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		file, _ := os.Open(filePath)
		file.Stat()
	}
}
