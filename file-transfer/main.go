package main

import (
	"os"
	"wangweizZZ/go-daily-study/file-transfer/cmd"

	"github.com/urfave/cli/v2"
)

func main() {

	app := &cli.App{
		Name:  "file transfer",
		Usage: "file transfer",
		Commands: []*cli.Command{
			&cmd.Server,
			&cmd.Client,
		},
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:  "debug",
				Usage: "enables debug logging",
			},
		},
	}
	app.Run(os.Args)
}
