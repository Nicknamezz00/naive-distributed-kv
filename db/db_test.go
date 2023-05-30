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

package db_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	internalDB "github.com/Nicknamezz00/naive-distributed-kv/db"
)

// Read only
func createTempDB(t *testing.T, readonly bool) *internalDB.Database {
	t.Helper()

	f, err := ioutil.TempFile(os.TempDir(), "dbtest")
	if err != nil {
		t.Fatalf("Cannot create temp db: %v", err)
	}

	name := f.Name()
	f.Close()
	t.Cleanup(func() { os.Remove(name) })

	db, closeFunc, err := internalDB.NewDatabase(name, readonly)
	if err != nil {
		t.Fatalf("Cannot create a new database: %v", err)
	}
	t.Cleanup(func() { closeFunc() })

	return db
}

func TestGetSet(t *testing.T) {
	db := createTempDB(t, false)
	if err := db.Set("hello", []byte("world")); err != nil {
		t.Fatalf("Cannot write key: %v", err)
	}
	value, err := db.Get("hello")
	if err != nil {
		t.Fatalf(`Cannot get key "hello": %v`, err)
	}
	if !bytes.Equal(value, []byte("world")) {
		t.Errorf(`Unexpected value for key "hello": got %q, want %q`, value, "world")
	}
}

func setKey(t *testing.T, d *internalDB.Database, key, value string) {
	t.Helper()
	if err := d.Set(key, []byte(value)); err != nil {
		t.Fatalf("SetKey(%q, %q) failed: %v", key, value, err)
	}
}

func getKey(t *testing.T, d *internalDB.Database, key string) string {
	t.Helper()
	value, err := d.Get(key)
	if err != nil {
		t.Fatalf("GetKey(%q) failed: %v", value, err)
	}
	return string(value)
}

func TestDeleteExtraKeys(t *testing.T) {
	db := createTempDB(t, false)
	setKey(t, db, "hello", "world")
	setKey(t, db, "merry", "christmas")

	if err := db.DeleteExtraKeys(func(name string) bool { return name == "merry" }); err != nil {
		t.Fatalf("Cannot delete extra keys: %v", err)
	}

	if value := getKey(t, db, "hello"); value != "world" {
		t.Errorf(`Unexpected value for key "hello": got %q, want %q`, value, "")
	}

	if value := getKey(t, db, "merry"); value != "" {
		t.Errorf(`Unexpected value for key "merry": got %q, want %q`, value, "")
	}
}

func TestSetOnReadOnly(t *testing.T) {
	db := createTempDB(t, true)
	if err := db.Set("foo", []byte("bar")); err == nil {
		t.Fatalf("SetOnReadOnly(%q, %q): got nil error, want non-nil error", "foo", []byte("bar"))
	}
}

func TestDeleteReplicaKey(t *testing.T) {
	db := createTempDB(t, false)
	setKey(t, db, "hello", "world")
	k, v, err := db.GetOldKey()
	if err != nil {
		t.Fatalf(`Unexpected error for GetOldKey(): %v`, err)
	}
	if !bytes.Equal(k, []byte("hello")) || !bytes.Equal(v, []byte("world")) {
		t.Errorf(`GetOldKey() returns unexpected key-value: got %q, %q; want %q, %q`, k, v, "hello", "world")
	}
	if err := db.DeleteReplicaKey([]byte("hello"), []byte("foo")); err == nil {
		t.Fatalf(`DeleteReplicaKey("hello", "foo"): got nil error, want non-nil error`)
	}
	if err := db.DeleteReplicaKey([]byte("hello"), []byte("world")); err != nil {
		t.Fatalf(`DeleteReplicaKey("hello", "world"): got %v, want nil error`, err)
	}
	// Now the previous `k` `v` should be deleted, there are not more keys to delete.
	k, v, err = db.GetOldKey()
	if err != nil {
		t.Fatalf(`Unexpected error for GetOldKey(): %v`, err)
	}
	if k != nil || v != nil {
		t.Errorf(`GetOldKey(): got %q, %q; want nil, nil`, k, v)
	}
}
