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

package db

import (
	"bytes"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

var (
	defaultBucket = []byte("default")
	replicaBucket = []byte("replica")
)

type Database struct {
	db       *bolt.DB
	readOnly bool
}

func NewDatabase(dbPath string, readOnly bool) (db *Database, closeFunc func() error, err error) {
	boltDB, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, nil, err
	}

	db = &Database{db: boltDB, readOnly: readOnly}
	closeFunc = boltDB.Close

	if err := db.createBucket(); err != nil {
		_ = closeFunc()
		return nil, nil, fmt.Errorf("error creating default bucket: %w", err)
	}

	return db, closeFunc, nil
}

func (d *Database) createBucket() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(defaultBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(replicaBucket); err != nil {
			return err
		}
		return nil
	})
}

// Set key
func (d *Database) Set(key string, value []byte) error {
	if d.readOnly {
		return errors.New("read-only mode")
	}
	return d.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(defaultBucket).Put([]byte(key), value); err != nil {
			return err
		}
		return tx.Bucket(replicaBucket).Put([]byte(key), value)
	})
}

// Get key
func (d *Database) Get(key string) ([]byte, error) {
	var result []byte
	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		result = copyByteSlice(b.Get([]byte(key)))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// DeleteExtraKeys deletes extra keys that do not belong to this shard.
func (d *Database) DeleteExtraKeys(isExtra func(string) bool) error {
	var keys []string

	err := d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		return b.ForEach(func(k, v []byte) error {
			kStr := string(k)
			if isExtra(kStr) {
				keys = append(keys, kStr)
			}
			return nil
		})
	})
	if err != nil {
		return err
	}

	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		for _, k := range keys {
			if err := b.Delete([]byte(k)); err != nil {
				return err
			}
		}
		return nil
	})
}

// SetReplica this function is intended to be used only on replicas.
// It sets the key value into default bucket without writes to replication queue.
func (d *Database) SetReplica(key string, value []byte) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(defaultBucket).Put([]byte(key), value)
	})
}

func copyByteSlice(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// GetOldKey returns key and value that have not been applied to replicas,
// if no such keys exist, returns nil key and nil value.
func (d *Database) GetOldKey() (key, value []byte, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicaBucket)
		k, v := b.Cursor().First()
		key = copyByteSlice(k)
		value = copyByteSlice(v)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return key, value, err
}

// DeleteReplicaKey deletes key from replication queue.
func (d *Database) DeleteReplicaKey(key, value []byte) (err error) {
	return d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicaBucket)
		v := b.Get(key)
		if v == nil {
			return errors.New("key does not exist")
		}
		if !bytes.Equal(v, value) {
			return errors.New("value does not exist")
		}
		return b.Delete(key)
	})
}
