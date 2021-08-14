package main

import (
	"flag"
	"wangweizZZ/kv/pkg/http"
)

var dir = flag.String("dir", "./data", "store dir location")

func main() {
	flag.Parse()
	s, err := http.NewServer(*dir)
	if err != nil {
		panic(err.Error)
	}
	s.Serve()
}
