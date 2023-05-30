#!/usr/bin/env bash

for shard in 127.0.0.1:3000; do
  echo $shard
  for i in {1..1000}; do
    curl "http://$shard/set?key=key-$RANDOM&value=value-$RANDOM"
  done
done
