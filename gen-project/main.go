package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
)

const (
	README = "README.md"
)

var need_dir = [...]string{"cmd", "pkg", "internal", "docs", "examples"}

func initDir() {
	for _, v := range need_dir {
		if _, e := os.Stat(v); os.IsNotExist(e) {
			os.Mkdir(v, os.ModePerm)
		}
	}
}

func createReadMe(project string) {
	ioutil.WriteFile(README, []byte("# "+project), 0644)
}

func main() {
	var projectName string

	var pwd, _ = os.Getwd()

	sp := strings.Split(pwd, string(os.PathSeparator))

	flag.StringVar(&projectName, "p", sp[len(sp)-1], "project name")
	flag.Parse()

	if len(projectName) == 0 {
		panic("miss project")
	}
	cmd := exec.Command("go", "mod", "init", projectName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		panic(err.Error() + ":" + string(output))
	}

	initDir()

	createReadMe(projectName)

	fmt.Println("init success:", projectName)
}
