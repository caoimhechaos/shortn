/*
 * (c) 2011, Caoimhe Chaos <caoimhechaos@protonmail.com>,
 *	     Ancient Solutions. All rights reserved.
 *
 * Redistribution and use in source  and binary forms, with or without
 * modification, are permitted  provided that the following conditions
 * are met:
 *
 * * Redistributions of  source code  must retain the  above copyright
 *   notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 *   notice, this  list of conditions and the  following disclaimer in
 *   the  documentation  and/or  other  materials  provided  with  the
 *   distribution.
 * * Neither  the  name  of  Ancient Solutions  nor  the  name  of its
 *   contributors may  be used to endorse or  promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS"  AND ANY EXPRESS  OR IMPLIED WARRANTIES  OF MERCHANTABILITY
 * AND FITNESS  FOR A PARTICULAR  PURPOSE ARE DISCLAIMED. IN  NO EVENT
 * SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL,  EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED  TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE,  DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT  LIABILITY,  OR  TORT  (INCLUDING NEGLIGENCE  OR  OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package main

import (
	"github.com/golang/protobuf/proto"

	"crypto/sha256"
	"database/cassandra"
	"encoding/base64"
	"errors"
	"expvar"
	"hash"
	"log"
	"time"
)

type CassandraStore struct {
	client *cassandra.RetryCassandraClient
	corpus string
	path   *cassandra.ColumnPath
}

var num_notfound *expvar.Int = expvar.NewInt("cassandra-not-found")
var num_errors *expvar.Map = expvar.NewMap("cassandra-errors")
var num_found *expvar.Int = expvar.NewInt("cassandra-found")

func NewCassandraStore(servaddr string, keyspace, corpus string) *CassandraStore {
	var err error
	var client *cassandra.RetryCassandraClient
	var path *cassandra.ColumnPath

	client, err = cassandra.NewRetryCassandraClientTimeout(servaddr,
		10*time.Second)
	if err != nil {
		log.Print("Error opening connection to ", servaddr, ": ", err)
		return nil
	}

	err = client.SetKeyspace(keyspace)
	if err != nil {
		log.Print("Error setting keyspace to ", corpus, ": ", err)
		return nil
	}

	path = cassandra.NewColumnPath()
	path.ColumnFamily = corpus
	path.SuperColumn = nil
	path.Column = []byte("url")

	conn := &CassandraStore{
		client: client,
		corpus: corpus,
		path:   path,
	}
	return conn
}

func (conn *CassandraStore) LookupURL(shortname string) (string, error) {
	// Ensure the connection is not currently being initialized.
	col, err := conn.client.Get([]byte(shortname), conn.path,
		cassandra.ConsistencyLevel_ONE)

	if col == nil {
		if err != nil {
			log.Print("Error getting column: ", err.Error(), "\n")
			num_errors.Add("os-error", 1)
			return "", err
		}

		return "", nil
	}

	num_found.Add(1)
	return string(col.Column.Value), nil
}

func (conn *CassandraStore) AddURL(url, owner string) (shorturl string, err error) {
	var col cassandra.Column
	var cp cassandra.ColumnParent
	var rmd hash.Hash = sha256.New()
	var digest string

	_, err = rmd.Write([]byte(url))
	if err != nil {
		return
	}

	digest = base64.URLEncoding.EncodeToString(rmd.Sum(nil))
	shorturl = digest[0:7]

	cp.ColumnFamily = conn.corpus
	cp.SuperColumn = nil

	col.Name = []byte("url")
	col.Value = []byte(url)
	col.Timestamp = proto.Int64(time.Now().Unix())
	col.TTL = proto.Int32(0)

	// TODO(caoimhe): Use a mutation pool and locking here!
	err = conn.client.Insert([]byte(shorturl), &cp, &col,
		cassandra.ConsistencyLevel_ONE)
	if err != nil {
		log.Println("Generic error: ", err)
		num_errors.Add("os-error", 1)
		err = errors.New(err.Error())
		return
	}
	col.Name = []byte("owner")
	col.Value = []byte(owner)
	err = conn.client.Insert([]byte(shorturl), &cp, &col,
		cassandra.ConsistencyLevel_ONE)
	if err != nil {
		log.Println("Generic error: ", err)
		num_errors.Add("os-error", 1)
		err = errors.New(err.Error())
		return
	}
	return "/" + shorturl, nil
}
