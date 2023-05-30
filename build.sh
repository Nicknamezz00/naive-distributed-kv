#!/usr/bin/env bash

#
# MIT License
#
# Copyright (c) 2023 Runze Wu
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
#

set -e

trap 'killall naive-distributed-kv' SIGINT

cd $(dirname "$0")

killall naive-distributed-kv || true
sleep 0.2

go install -v

naive-distributed-kv -http-addr=127.0.0.1:3000 -path=./db0.db -shard=Node0 &
naive-distributed-kv -http-addr=127.0.0.1:3001 -path=./db0-replica.db -shard=Node0 -replica &

naive-distributed-kv -http-addr=127.0.0.1:3100 -path=./db1.db -shard=Node1 &
naive-distributed-kv -http-addr=127.0.0.1:3101 -path=./db1-replica.db -shard=Node1 -replica &

naive-distributed-kv -http-addr=127.0.0.1:3200 -path=./db2.db -shard=Node2 &
naive-distributed-kv -http-addr=127.0.0.1:3201 -path=./db2-replica.db -shard=Node2 -replica &

naive-distributed-kv -http-addr=127.0.0.1:3300 -path=./db3.db -shard=Node3 &
naive-distributed-kv -http-addr=127.0.0.1:3301 -path=./db3-replica.db -shard=Node3 -replica &

wait
