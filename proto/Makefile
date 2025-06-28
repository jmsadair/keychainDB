.PHONY: proto all

all: proto

proto:
	protoc chain.proto --go_out=./pbchain --go-grpc_out=./pbchain --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
