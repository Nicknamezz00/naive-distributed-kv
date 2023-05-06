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
	internalDB "github.com/Nicknamezz00/naive-distributed-kv/db"
	"io/ioutil"
	"os"
	"testing"
)

func TestGetSet(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "dbtest")
	if err != nil {
		t.Fatalf("Cannot create temp db: %v", err)
	}

	name := f.Name()
	f.Close()
	defer os.Remove(name)

	db, closeFunc, err := internalDB.NewDatabase(name)
	if err != nil {
		t.Fatalf("Cannot create a new database: %v", err)
	}
	defer closeFunc()

	setKey(t, db, "Hello", "World")
	value := getKey(t, db, "Hello")
	if value != "World" {
		t.Errorf(`Unexpected value for key "Hello": got %q, want %q`, value, "World")
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
	f, err := ioutil.TempFile(os.TempDir(), "dbtest")
	if err != nil {
		t.Fatalf("Cannot create temp db: %v", err)
	}

	name := f.Name()
	f.Close()
	defer os.Remove(name)

	db, closeFunc, err := internalDB.NewDatabase(name)
	if err != nil {
		t.Fatalf("Cannot create a new database: %v", err)
	}
	defer closeFunc()

	setKey(t, db, "Hello", "World")
	setKey(t, db, "Merry", "Christmas")
	if err := db.DeleteExtraKeys(func(name string) bool { return name == "Merry" }); err != nil {
		t.Fatalf("Cannot delete extra keys: %v", err)
	}

	if value := getKey(t, db, "Hello"); value != "World" {
		t.Errorf(`Unexpected value for key "Hello": got %q, want %q`, value, "")
	}

	if value := getKey(t, db, "Merry"); value != "" {
		t.Errorf(`Unexpected value for key "Merry": got %q, want %q`, value, "")
	}
}
