# file-transfer

Demo for transferring files via grpc 

## feature 
- [x] transfer file
- [x] resume transfer
- [x] support tls
- [ ] md5 check

## how to use
1. Run Server `go run main.go server`
2. Run Client `go run main.go client -file xxxx`

## generate code
protoc --go_out=. --go_opt=paths=source_relative  --go-grpc_out=. --go-grpc_opt=paths=source_relative internal/proto/service.proto

