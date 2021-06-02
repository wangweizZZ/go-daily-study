package internal

import (
	"os"
	"strings"
)

func Size(path string) (int64, error) {
	fstat, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	fsize := fstat.Size()
	return fsize, nil
}

func GetName(path string) string {
	if strings.Contains(path, string(os.PathSeparator)) {
		sp := strings.Split(path, string(os.PathSeparator))
		return sp[len(sp)-1]
	}
	return path
}
