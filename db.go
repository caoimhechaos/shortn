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
	"crypto/sha256"
	"database/cassandra"
	"encoding/base64"
	"errors"
	"expvar"
	"hash"
	"log"
	"sync"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
)

type CassandraStore struct {
	client           *cassandra.CassandraClient
	addr             string
	corpus           string
	path             *cassandra.ColumnPath
	protocolFactory  *thrift.TBinaryProtocolFactory
	socket           *thrift.TSocket
	transportFactory thrift.TTransportFactory
	transport        thrift.TTransport
	mtx              sync.RWMutex
}

var num_notfound *expvar.Int = expvar.NewInt("cassandra-not-found")
var num_errors *expvar.Map = expvar.NewMap("cassandra-errors")
var num_found *expvar.Int = expvar.NewInt("cassandra-found")

func NewCassandraStore(servaddr string, corpus string) *CassandraStore {
	var err error
	var socket *thrift.TSocket

	socket, err = thrift.NewTSocket(servaddr)
	if err != nil {
		log.Print("Error opening connection to ", servaddr, ": ", err)
		return nil
	}

	conn := &CassandraStore{
		corpus:           corpus,
		addr:             servaddr,
		protocolFactory:  thrift.NewTBinaryProtocolFactoryDefault(),
		socket:           socket,
		transportFactory: thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory()),
	}
	if err = conn.Initialize(); err != nil {
		return nil
	}

	return conn
}

func (conn *CassandraStore) Initialize() error {
	var err error

	// Ensure we're undisturbed.
	conn.mtx.Lock()
	defer conn.mtx.Unlock()

	// It's possible that someone else discovered the disconnection before.
	if conn.socket.IsOpen() {
		return nil
	}

	if err = conn.socket.Open(); err != nil {
		log.Print("Error opening connection to ", conn.addr, ": ",
			err.Error(), "\n")
		return err
	}

	conn.transport = conn.transportFactory.GetTransport(conn.socket)
	conn.client = cassandra.NewCassandraClientFactory(conn.transport,
		conn.protocolFactory)

	if _, err = conn.client.SetKeyspace("shortn"); err != nil {
		log.Print("Error setting keyspace: ", err.Error(), "\n")
		return err
	}

	conn.path = cassandra.NewColumnPath()
	conn.path.ColumnFamily = conn.corpus
	conn.path.SuperColumn = nil
	conn.path.Column = []byte("url")
	return nil
}

func (conn *CassandraStore) LookupURL(shortname string) string {
	var err error

	// Ensure the connection is not currently being initialized.
	conn.mtx.RLock()

	// If the connection is broken, reinitialize it.
	for !conn.socket.IsOpen() {
		conn.mtx.RUnlock()
		err = conn.Initialize()
		for err != nil {
			log.Print("Retrying...")
			err = conn.Initialize()
		}
		conn.mtx.RLock()
	}
	defer conn.mtx.RUnlock()

	col, ire, nfe, ue, te, err := conn.client.Get([]byte(shortname),
		conn.path, cassandra.ConsistencyLevel_ONE)

	if col == nil {
		if ire != nil {
			log.Println("Invalid request: ", ire.Why)
			num_errors.Add("invalid-request", 1)
		}

		if nfe != nil {
			num_notfound.Add(1)
		}

		if ue != nil {
			log.Println("Unavailable")
			num_errors.Add("unavailable", 1)
		}

		if te != nil {
			log.Println("Request to database backend timed out")
			num_errors.Add("timeout", 1)
		}

		if err != nil {
			log.Print("Error getting column: ", err.Error(), "\n")
			num_errors.Add("os-error", 1)
		}

		return ""
	}

	num_found.Add(1)
	return string(col.Column.Value)
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
	col.Timestamp = time.Now().Unix()
	col.Ttl = 0

	// Ensure the connection is not currently being initialized.
	conn.mtx.RLock()

	// If the connection is broken, reinitialize it.
	for !conn.socket.IsOpen() {
		conn.mtx.RUnlock()
		err = conn.Initialize()
		for err != nil {
			log.Print("Retrying...")
			err = conn.Initialize()
		}
		conn.mtx.RLock()
	}
	defer conn.mtx.RUnlock()

	// TODO(caoimhe): Use a mutation pool and locking here!
	ire, ue, te, err := conn.client.Insert([]byte(shorturl), &cp, &col,
		cassandra.ConsistencyLevel_ONE)
	if ire != nil {
		log.Println("Invalid request: ", ire.Why)
		num_errors.Add("invalid-request", 1)
		err = errors.New(ire.String())
		return
	}
	if ue != nil {
		log.Println("Unavailable")
		num_errors.Add("unavailable", 1)
		err = errors.New(ue.String())
		return
	}
	if te != nil {
		log.Println("Request to database backend timed out")
		num_errors.Add("timeout", 1)
		err = errors.New(te.String())
		return
	}
	if err != nil {
		log.Println("Generic error: ", err)
		num_errors.Add("os-error", 1)
		err = errors.New(err.Error())
		return
	}
	col.Name = []byte("owner")
	col.Value = []byte(owner)
	ire, ue, te, err = conn.client.Insert([]byte(shorturl), &cp, &col,
		cassandra.ConsistencyLevel_ONE)
	if ire != nil {
		log.Println("Invalid request: ", ire.Why)
		num_errors.Add("invalid-request", 1)
		err = errors.New(ire.String())
		return
	}
	if ue != nil {
		log.Println("Unavailable")
		num_errors.Add("unavailable", 1)
		err = errors.New(ue.String())
		return
	}
	if te != nil {
		log.Println("Request to database backend timed out")
		num_errors.Add("timeout", 1)
		err = errors.New(te.String())
		return
	}
	if err != nil {
		log.Println("Generic error: ", err)
		num_errors.Add("os-error", 1)
		err = errors.New(err.Error())
		return
	}
	return "/" + shorturl, nil
}
