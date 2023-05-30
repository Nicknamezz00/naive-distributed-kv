/*
 * MIT License
 *
 * Copyright (c) 2023 Runze Wu
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package main

import (
	"flag"
	"log"
	"net/http"

	internalReplica "github.com/Nicknamezz00/naive-distributed-kv/replica"

	"github.com/Nicknamezz00/naive-distributed-kv/api"
	"github.com/Nicknamezz00/naive-distributed-kv/config"
	internalDB "github.com/Nicknamezz00/naive-distributed-kv/db"
)

var (
	dbPath     = flag.String("path", "", "The path to bolt db")
	httpAddr   = flag.String("http-addr", "127.0.0.1:8080", "HTTP address listening")
	configFile = flag.String("config", "sharding.toml", "Config for static sharding")
	shard      = flag.String("shard", "", "The name of the shard for the data")
	replica    = flag.Bool("replica", false, "Run as a read-only replica or not")
)

func parseFlags() {
	flag.Parse()

	if *dbPath == "" {
		log.Fatalf("Must provide db path")
	}
	if *shard == "" {
		log.Fatalf("Must provide shard")
	}
}

func main() {
	parseFlags()

	cfg, err := config.ParseFile(*configFile)
	if err != nil {
		log.Fatalf("Error parsing config %q: %v", *configFile, err)
	}

	shards, err := config.ParseShards(cfg.Shards, *shard)
	if err != nil {
		log.Fatalf("Error parsing shards config: %v", err)
	}
	log.Printf("Shard count is %d, current shard: %d", shards.Count, shards.CurIdx)

	db, closeFunc, err := internalDB.NewDatabase(*dbPath, *replica)
	if err != nil {
		log.Fatalf("NewDatabase(%q): %v", *dbPath, err)
	}
	defer closeFunc()

	if *replica {
		leader, ok := shards.Addrs[shards.CurIdx]
		if !ok {
			log.Fatalf("Cannot find address for leader shard %d", shards.CurIdx)
		}
		go internalReplica.ClientLoop(db, leader)
	}

	srv := api.NewServer(db, shards)

	http.HandleFunc("/get", srv.GetHandler)
	http.HandleFunc("/set", srv.SetHandler)
	http.HandleFunc("/delete-extra", srv.DeleteExtraKeysHandler)
	http.HandleFunc("/delete-replica-key", srv.DeleteReplicaKey)
	http.HandleFunc("/get-old-key", srv.GetOldKey)

	log.Fatal(http.ListenAndServe(*httpAddr, nil))
}
