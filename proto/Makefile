.PHONY: proto all

all: proto

proto:
	protoc chain.proto --go_out=./pbchain --go-grpc_out=./pbchain --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
	protoc storage.proto --go_out=./pbstorage --go-grpc_out=./pbstorage --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
	protoc coordinator.proto --go_out=./pbcoordinator --go-grpc_out=./pbcoordinator --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
