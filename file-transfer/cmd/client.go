package cmd

import (
	"log"
	"wangweizZZ/go-daily-study/file-transfer/internal"

	"github.com/urfave/cli/v2"
)

var Client = cli.Command{
	Name:   "client",
	Usage:  "run transfer client",
	Action: clientAction,
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "server_tls",
			Usage: "Connection uses TLS if true, else plain TCP",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "ca_file",
			Usage: "The TLS cert file",
		},
		&cli.StringFlag{
			Name:  "server_host_override",
			Usage: "The server name used to verify the hostname returned by the TLS handshake",
		},
		&cli.StringFlag{
			Name:  "server_addr",
			Usage: "The transfer address",
			Value: "localhost:10000",
		},
		&cli.StringFlag{
			Name:  "file",
			Usage: "The transfer file",
			Value: "",
		},
	},
}

func clientAction(c *cli.Context) (err error) {
	var (
		clientTls          = c.Bool("client_tls")
		caFile             = c.String("ca_file")
		serverAddr         = c.String("server_addr")
		serverHostOverride = c.String("server_host_override")
		file               = c.String("file")
	)

	var client internal.Client
	if clientTls {
		client = internal.NewGrpcClient(serverAddr, internal.NewClientConfig(internal.WithClientTls(caFile, serverHostOverride)))
	} else {
		client = internal.NewGrpcClient(serverAddr, internal.DefaultClientConfig)
	}
	err = client.Transfer(file)
	if err != nil {
		panic(err.Error())
	}
	log.Println("tansfer finish")
	return
}
