package internal

import (
	"context"
	"io"
	"log"
	"os"
	"time"
	"wangweizZZ/go-daily-study/file-transfer/internal/proto"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var _ Client = &grpcClient{}

type grpcClientOption func(*grpcClient)

type grpcClient struct {
	config      *clientConfig
	address     string
	conn        *grpc.ClientConn
	innerClient proto.TransferServiceClient
}

type clientConfig struct {
	tls                bool
	caFile             string
	serverHostOverride string
}

type ClientOption func(*clientConfig)

func WithClientTls(caFile string, serverHostOverride string) ClientOption {
	return func(cc *clientConfig) {
		cc.tls = true
		cc.caFile = caFile
		cc.serverHostOverride = serverHostOverride
	}
}

func NewClientConfig(opts ...ClientOption) *clientConfig {
	clientConfig := &clientConfig{
		tls: false,
	}
	for _, opt := range opts {
		opt(clientConfig)
	}
	return clientConfig
}

//default tls is false
var DefaultClientConfig *clientConfig = &clientConfig{tls: false}

func NewGrpcClient(add string, config *clientConfig) *grpcClient {
	return &grpcClient{
		config:  config,
		address: add,
	}
}

func (c *grpcClient) Transfer(filePath string) error {
	fsize, err := Size(filePath)
	if err != nil {
		return err
	}

	//readfile
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	//init connection
	c.initConn()
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	//
	fir, err := c.doOpen(ctx, file.Name(), true)
	if err != nil {
		return err
	}
	var offset int64 = 0
	if fir.GetOffset() != 0 {
		if fir.GetOffset() > fsize {
			return errors.New("seek offset is too big")
		} else if fir.GetOffset() == fsize {
			return nil
		}

		offset, err = file.Seek(fir.GetOffset(), 0)
		if err != nil {
			return err
		}
	}

	for {
		status, err := c.doTransfer(ctx, file, fir.GetId(), offset)
		if err != nil {
			return err
		}
		switch status.Code {
		case proto.ResultCode_Ok:
			return nil
		case proto.ResultCode_Unknown:
			if fsize == status.Offset {
				return nil
			} else if fsize < status.Offset {
				return errors.New("file size wrong!" + status.Message)
			}
		default:
			return errors.New("fail:" + status.Message)
		}
	}
}

func (c *grpcClient) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *grpcClient) doTransfer(ctx context.Context, file *os.File, id string, offset int64) (*proto.ChunkResult, error) {
	stream, err := c.innerClient.Write(ctx)
	if err != nil {
		return nil, err
	}
	defer stream.CloseSend()
	buf := make([]byte, 20)

	log.Println("start transfer from", offset)
	var num int
	for {
		num, err = file.Read(buf)
		if err != nil {
			if err == io.EOF {
				return stream.CloseAndRecv()
			}
		}
		offset += int64(num)
		stream.Send(&proto.Chunk{
			Offset:  offset,
			Id:      id,
			Content: buf[:num],
		})
	}
}

func (c *grpcClient) initConn() error {
	var opts []grpc.DialOption
	var err error

	config := c.config
	if config.tls {
		creds, err := credentials.NewClientTLSFromFile(config.caFile, config.serverHostOverride)
		if err != nil {
			return err
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}
	opts = append(opts, grpc.WithBlock())

	c.conn, err = grpc.Dial(c.address, opts...)
	if err != nil {
		return err
	}
	c.innerClient = proto.NewTransferServiceClient(c.conn)
	return nil
}

func (c *grpcClient) doOpen(ctx context.Context, fname string, append bool) (*proto.FileInfoResult, error) {
	return c.innerClient.Open(ctx, &proto.FileInfo{
		Name:   GetName(fname),
		Size:   1024, //todo size check
		Md5:    "",   //todo md5 check
		Append: append,
	})
}
