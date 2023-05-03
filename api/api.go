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

package api

import (
	"fmt"
	"github.com/Nicknamezz00/naive-distributed-kv/db"
	"hash/fnv"
	"io"
	"net/http"
)

type Server struct {
	db         *db.Database
	shardIdx   int
	shardCount int
	addrs      map[int]string
}

func NewServer(db *db.Database, shardIdx int, shardCount int, addrs map[int]string) *Server {
	return &Server{
		db:         db,
		shardIdx:   shardIdx,
		shardCount: shardCount,
		addrs:      addrs,
	}
}

func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")

	shard := s.getShard(key)
	if shard != s.shardIdx {
		s.redirect(shard, w, r)
		return
	}
	value, err := s.db.Get(key)
	fmt.Fprintf(w, "Shard = %d, current = %d, addr = %q, Value = %q, error = %v", shard, s.shardIdx, s.addrs[shard], value, err)
}

func (s *Server) SetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	value := r.Form.Get("value")

	shard := s.getShard(key)
	if shard != s.shardIdx {
		s.redirect(shard, w, r)
		return
	}

	err := s.db.Set(key, []byte(value))
	fmt.Fprintf(w, "Error = %v, shard = %d", err, shard)
}

func (s *Server) getShard(key string) int {
	h := fnv.New64()
	h.Write([]byte(key))
	return int(h.Sum64() % uint64(s.shardCount))
}

func (s *Server) ListenAndServe(addr string) error {
	return http.ListenAndServe(addr, nil)
}

func (s *Server) redirect(shard int, w http.ResponseWriter, r *http.Request) {
	url := "http://" + s.addrs[shard] + r.RequestURI
	resp, err := http.Get(url)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error redirecting request: %v", err)
		return
	}
	defer resp.Body.Close()

	io.Copy(w, resp.Body)
}
