# Service

## Run client

```bash
go run cmd/client/main.go
```

## Run server

```bash
go run main.go
```

## Generate code

```bash
mkdir -p protos
protoc \
  --go_out=./protos/ \
  --go_opt="Mecommerce/v1/product_info.proto=product_info/v1;product_info" \
  --go-grpc_out=./protos/ \
  --go-grpc_opt="Mecommerce/v1/product_info.proto=product_info/v1;product_info" \
  ecommerce/v1/product_info.proto
```
