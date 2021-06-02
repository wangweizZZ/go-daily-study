package cmd

import (
	"wangweizZZ/go-daily-study/file-transfer/internal"

	"github.com/urfave/cli/v2"
)

var Server = cli.Command{
	Name:   "server",
	Usage:  "run transfer server",
	Action: serverAction,
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "server_tls",
			Usage: "Connection uses TLS if true, else plain TCP",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "cert_file",
			Usage: "The TLS cert file",
		},
		&cli.StringFlag{
			Name:  "key_file",
			Usage: "The TLS key file",
		},
		&cli.StringFlag{
			Name:  "listen",
			Usage: "The listen address",
			Value: "localhost:10000",
		},
	},
}

func serverAction(c *cli.Context) (err error) {
	var (
		serverTls = c.Bool("server_tls")
		certFile  = c.String("cert_file")
		keyFile   = c.String("key_file")
		listen    = c.String("listen")
	)

	var server internal.Server
	if serverTls {
		server = internal.NewGrpcServer(listen, internal.NewServerConfig(internal.WithServerTls(certFile, keyFile)))
	} else {
		server = internal.NewGrpcServer(listen, internal.DefaultServerConfig)
	}
	err = server.Start()
	defer server.Close()
	return
}
