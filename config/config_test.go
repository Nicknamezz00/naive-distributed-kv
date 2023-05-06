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

package config_test

import (
	"github.com/Nicknamezz00/naive-distributed-kv/config"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

func createConfig(t *testing.T, contents string) config.Config {
	t.Helper()
	f, err := ioutil.TempFile(os.TempDir(), "config.toml")
	if err != nil {
		t.Fatalf("Cannot create a temp file: %v", err)
	}

	name := f.Name()
	defer f.Close()
	defer os.Remove(name)

	_, err = f.WriteString(contents)
	if err != nil {
		t.Fatalf("Cannot write contents into config: %v", err)
	}

	c, err := config.ParseFile(name)
	if err != nil {
		t.Fatalf("Cannot parse config: %v", err)
	}
	return c
}

func TestConfigParse(t *testing.T) {
	got := createConfig(t, `[[shards]]
		name = "NodeTest"
		idx = 0
		address = "localhost:8080"`)

	want := config.Config{
		Shards: []config.Shard{
			{
				Name:    "NodeTest",
				Idx:     0,
				Address: "localhost:8080",
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("The config does not match, got: %#v, but want: %#v", got, want)
	}
}

func TestParseShards(t *testing.T) {
	c := createConfig(t, `
	[[shards]]
		name = "NodeTest0"
		idx = 0
		address = "localhost:8080"
	[[shards]]
		name = "NodeTest1"
		idx = 1
		address = "localhost:8081"`)

	got, err := config.ParseShards(c.Shards, "NodeTest1")
	if err != nil {
		t.Fatalf("Cannot parse shards %#v: %v", c.Shards, err)
	}
	want := &config.Shards{
		Count:  2,
		CurIdx: 1,
		Addrs: map[int]string{
			0: "localhost:8080",
			1: "localhost:8081",
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("The shards does not match, got: %#v, but want: %#v", got, want)
	}
}
