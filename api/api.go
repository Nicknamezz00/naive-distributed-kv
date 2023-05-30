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
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/Nicknamezz00/naive-distributed-kv/config"
	"github.com/Nicknamezz00/naive-distributed-kv/db"
	"github.com/Nicknamezz00/naive-distributed-kv/replica"
)

type Server struct {
	db     *db.Database
	shards *config.Shards
}

func NewServer(db *db.Database, s *config.Shards) *Server {
	return &Server{
		db:     db,
		shards: s,
	}
}

func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")

	shard := s.shards.Index(key)
	if shard != s.shards.CurIdx {
		s.redirect(shard, w, r)
		return
	}

	value, err := s.db.Get(key)
	fmt.Fprintf(w, "Shard = %d, current = %d, addr = %q, Value = %q, error = %v\n", shard, s.shards.CurIdx, s.shards.Addrs[shard], value, err)
}

func (s *Server) SetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	value := r.Form.Get("value")

	shard := s.shards.Index(key)
	if shard != s.shards.CurIdx {
		s.redirect(shard, w, r)
		return
	}

	err := s.db.Set(key, []byte(value))
	fmt.Fprintf(w, "Error = %v, shard = %d, current shard = %d", err, shard, s.shards.CurIdx)
}

func (s *Server) DeleteExtraKeysHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Error = %v", s.db.DeleteExtraKeys(func(key string) bool {
		return s.shards.Index(key) != s.shards.CurIdx
	}))
}

func (s *Server) ListenAndServe(addr string) error {
	return http.ListenAndServe(addr, nil)
}

func (s *Server) redirect(shard int, w http.ResponseWriter, r *http.Request) {
	url := "http://" + s.shards.Addrs[shard] + r.RequestURI
	fmt.Fprintf(w, "\nredirecting from shard %d to shard %d (%q)\n", s.shards.CurIdx, shard, url)

	resp, err := http.Get(url)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Error redirecting request: %v", err)
		return
	}
	defer resp.Body.Close()

	io.Copy(w, resp.Body)
}

func (s *Server) GetOldKey(w http.ResponseWriter, r *http.Request) {
	e := json.NewEncoder(w)
	k, v, err := s.db.GetOldKey()
	e.Encode(&replica.NextKeyValue{
		Key:   string(k),
		Value: string(v),
		Err:   err,
	})
}

func (s *Server) DeleteReplicaKey(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	value := r.Form.Get("value")
	if err := s.db.DeleteReplicaKey([]byte(key), []byte(value)); err != nil {
		w.WriteHeader(http.StatusExpectationFailed)
		fmt.Fprintf(w, "error: %v", err)
		return
	}
	fmt.Fprintf(w, "ok")
}
