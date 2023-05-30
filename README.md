## Quickstart

### Build
```shell
sh ./build.sh
sh ./bootstrap.sh
```
It could take a few minutes, wait until it's done populating data.

### Benchmark
```shell
go run cmd/benchmark/main.go -concurrency=16 -iterations=1000
```

### Test
```shell
go run test ./... -v -race
```