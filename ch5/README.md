# Service

## Run server

```bash
go run *.go
```

## Generate code

```bash
mkdir -p protos
protoc \
  --go_out=./protos/ \
  --go-grpc_out=./protos/ \
  ecommerce/v1/order_management.proto
```


