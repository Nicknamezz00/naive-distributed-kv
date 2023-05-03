#!/usr/bin/env bash

set -e

trap 'killall naive-distributed-kv' SIGINT

cd $(dirname "$0")

killall naive-distributed-kv || true
sleep 0.2

go install -v

naive-distributed-kv -http-addr=127.0.0.1:8080 -path=./db0.db -shard=Node0 &
naive-distributed-kv -http-addr=127.0.0.1:8081 -path=./db1.db -shard=Node1 &
naive-distributed-kv -http-addr=127.0.0.1:8082 -path=./db2.db -shard=Node2 &

wait
