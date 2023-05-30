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

package replica

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/Nicknamezz00/naive-distributed-kv/db"
)

// NextKeyValue contains the response for GetNextKeyForReplication.
type NextKeyValue struct {
	Key   string
	Value string
	Err   error
}

type client struct {
	db     *db.Database
	leader string // http url of leader node
}

// ClientLoop continuously downloads new keys from the master and applies them.
func ClientLoop(db *db.Database, leader string) {
	c := &client{db: db, leader: leader}
	for {
		present, err := c.loop()
		if err != nil {
			log.Printf("Loop error: %v", err)
			time.Sleep(time.Second)
			continue
		}
		if !present {
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (c *client) loop() (present bool, err error) {
	resp, err := http.Get("http://" + c.leader + "/get-old-key")
	if err != nil {
		return false, err
	}

	var res NextKeyValue
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if res.Err != nil {
		return false, err
	}
	if res.Key == "" {
		return false, nil
	}

	if err := c.db.SetReplica(res.Key, []byte(res.Value)); err != nil {
		return false, err
	}
	if err := c.deleteFromReplicationQueue(res.Key, res.Value); err != nil {
		log.Printf("DeleteKeyFromReplication failed: %v", err)
	}

	return true, nil
}

func (c *client) deleteFromReplicationQueue(key, value string) error {
	u := url.Values{}
	u.Set("key", key)
	u.Set("value", value)

	log.Printf("Deleting key=%q, value=%q from replica queue on %q", key, value, c.leader)

	resp, err := http.Get("http://" + c.leader + "/delete-replica-key?" + u.Encode())
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if !bytes.Equal(result, []byte("ok")) {
		return errors.New(string(result))
	}

	return nil
}
