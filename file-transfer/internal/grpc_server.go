package internal

import (
	"context"
	"io"
	"log"
	"net"
	"os"
	"wangweizZZ/go-daily-study/file-transfer/internal/proto"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	tmp_path        string = "./tmp/"
	tmp_file_suffix string = ".tmp"
)

var _ Server = &grpcServer{}

type grpcServer struct {
	config      *serverConfig
	address     string
	innerServer *grpc.Server
	proto.UnimplementedTransferServiceServer
}

type serverConfig struct {
	tls      bool
	certFile string
	key      string
	store    string
}

type ServerOption func(*serverConfig)

func WithServerTls(certFile string, key string) ServerOption {
	return func(sc *serverConfig) {
		sc.tls = true
		sc.certFile = certFile
		sc.key = key
	}
}

func WithServerStore(store string) ServerOption {
	return func(sc *serverConfig) {
		sc.store = store
	}
}

func NewServerConfig(opts ...ServerOption) *serverConfig {
	serverConfig := &serverConfig{
		tls:   false,
		store: tmp_path,
	}
	for _, opt := range opts {
		opt(serverConfig)
	}
	return serverConfig
}

var DefaultServerConfig *serverConfig = &serverConfig{
	tls:   false,
	store: tmp_path,
}

func NewGrpcServer(add string, conf *serverConfig) *grpcServer {
	return &grpcServer{
		config:  conf,
		address: add,
	}
}

func (s *grpcServer) Open(ctx context.Context, finfo *proto.FileInfo) (*proto.FileInfoResult, error) {
	//check arg
	localFile, err := s.readyLocalFile(finfo.GetName())
	if err != nil {
		return nil, err
	}
	defer func() {
		if localFile != nil {
			localFile.Close()
		}
	}()

	var offset int64
	if finfo.GetAppend() {
		offset, err = localFile.Seek(0, 2)
		if err != nil {
			return nil, err
		}
	} else {
		localFile.Truncate(0)
	}

	return &proto.FileInfoResult{
		Id:     finfo.GetName(),
		Offset: offset,
	}, nil
}

func (s *grpcServer) Write(stream proto.TransferService_WriteServer) error {
	var localFile *os.File
	defer func() {
		if localFile != nil {
			localFile.Close()
		}
	}()

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			//todo check md5

			size, err := localFile.Seek(0, 2)
			if err != nil {
				return nil
			}
			if localFile != nil {
				err = os.Rename(localFile.Name(), localFile.Name()[:len(localFile.Name())-len(tmp_file_suffix)])
				if err != nil {
					return err
				}
			}
			return stream.SendAndClose(&proto.ChunkResult{
				Offset: size,
				Code:   proto.ResultCode_Ok,
			})
		}

		if err != nil {
			return err
		}

		if localFile == nil {
			localFile, err = s.readyLocalFile(in.GetId())
			if err != nil {
				return err
			}
			if _, err = localFile.Seek(0, 2); err != nil {
				return err
			}
		}

		if _, err = localFile.Write([]byte(in.Content)); err != nil {
			return err
		}
	}
}

func (s *grpcServer) Start() error {
	lis, err := net.Listen("tcp", s.address)

	if err != nil {
		return errors.Wrapf(err, "failed to listen on address %s", s.address)
	}
	sc := s.config
	var opts []grpc.ServerOption
	if sc.tls {
		altsTC, err := credentials.NewServerTLSFromFile(sc.certFile, sc.key)
		if err != nil {
			return err
		}
		grpc.Creds(altsTC)
	}

	s.innerServer = grpc.NewServer(opts...)
	proto.RegisterTransferServiceServer(s.innerServer, s)

	if _, err = os.Stat(tmp_path); os.IsNotExist(err) {
		err = os.Mkdir(tmp_path, 0777)
	}
	if err != nil {
		return err
	}

	log.Println("start to server Listen:", s.address)
	if err := s.innerServer.Serve(lis); err != nil {
		return errors.Wrapf(err, "failed listening connections")
	}
	return nil
}

func (s *grpcServer) Close() {
	if s.innerServer != nil {
		s.innerServer.Stop()
	}
}

func (s *grpcServer) readyLocalFile(fileName string) (*os.File, error) {
	return os.OpenFile(s.config.store+fileName+tmp_file_suffix, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
}
