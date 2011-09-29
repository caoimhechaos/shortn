/*
 * (c) 2011, Tonnerre Lombard <tonnerre@ancient-solutions.com>,
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

package main;

import (
	"crypto/ripemd160"
	"encoding/base64"
	"expvar"
	"hash"
	"log"
	"net"
	"os"
	"time"
	"thrift"
	"thriftlib/Cassandra"
)

type CassandraStore struct {
	Addr *net.TCPAddr;
	Client *Cassandra.CassandraClient;
	Corpus string;
	Path *Cassandra.ColumnPath;
	ProtocolFactory *thrift.TBinaryProtocolFactory;
	Socket *thrift.TSocket;
	TransportFactory thrift.TTransportFactory;
	Transport thrift.TTransport;
}

var num_notfound *expvar.Int = expvar.NewInt("cassandra-not-found")
var num_errors *expvar.Map = expvar.NewMap("cassandra-errors")
var num_found *expvar.Int = expvar.NewInt("cassandra-found")

func NewCassandraStore(servaddr string, corpus string) *CassandraStore {
	var err os.Error
	conn := new(CassandraStore)
	conn.Corpus = corpus
	conn.ProtocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
	conn.TransportFactory = thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	conn.Addr, err = net.ResolveTCPAddr("tcp", servaddr)

	if err != nil {
		log.Print("Error opening connection for protocol ", conn.Addr.Network(),
			" to ", conn.Addr.String(), ": ", err.String(), "\n")
		return nil
	}

	conn.Socket = thrift.NewTSocketAddr(conn.Addr)
	if err = conn.Socket.Open(); err != nil {
		log.Print("Error opening connection for protocol ", conn.Addr.Network(),
			" to ", conn.Addr.String(), ": ", err.String(), "\n")
		return nil
	}

	conn.Transport = conn.TransportFactory.GetTransport(conn.Socket)
	conn.Client = Cassandra.NewCassandraClientFactory(conn.Transport, conn.ProtocolFactory)

	if _, err = conn.Client.SetKeyspace("shortn"); err != nil {
		log.Print("Error setting keyspace: ", err.String(), "\n")
		os.Exit(2)
	}

	conn.Path = Cassandra.NewColumnPath()
	conn.Path.ColumnFamily = corpus
	conn.Path.SuperColumn__isset = false
	conn.Path.Column = "url"
	conn.Path.Column__isset = true

	return conn
}

func (conn *CassandraStore) LookupURL(shortname string) string {
	col, ire, nfe, ue, te, err := conn.Client.Get(shortname, conn.Path, Cassandra.ONE)

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
			log.Print("Error getting column: ", err.String(), "\n")
			num_errors.Add("os-error", 1)
		}

		return ""
	}

	num_found.Add(1)
	return col.Column.Value
}

func (conn *CassandraStore) AddURL(url, owner string) (shorturl string, err os.Error) {
	var col Cassandra.Column
	var cp Cassandra.ColumnParent
	var rmd hash.Hash = ripemd160.New()
	var digest string

	_, err = rmd.Write([]byte(url))
	if err != nil {
		return
	}

	digest = base64.URLEncoding.EncodeToString(rmd.Sum())
	shorturl = digest[0:7]

	cp.ColumnFamily = conn.Corpus
	cp.SuperColumn__isset = false

	col.Name = "url"
	col.Value = url
	col.Timestamp = time.Nanoseconds()
	col.Ttl__isset = false

	// TODO(tonnerre): Use a mutation pool and locking here!
	ire, ue, te, err := conn.Client.Insert(shorturl, &cp, &col, Cassandra.ONE)
	if ire != nil {
		log.Println("Invalid request: ", ire.Why)
		num_errors.Add("invalid-request", 1)
		err = os.NewError(ire.String())
		return
	}
	if ue != nil {
		log.Println("Unavailable")
		num_errors.Add("unavailable", 1)
		err = os.NewError(ue.String())
		return
	}
	if te != nil {
		log.Println("Request to database backend timed out")
		num_errors.Add("timeout", 1)
		err = os.NewError(te.String())
		return
	}
	if err != nil {
		log.Println("Generic error: ", err)
		num_errors.Add("os-error", 1)
		err = os.NewError(err.String())
		return
	}
	col.Name = "owner"
	col.Value = owner
	ire, ue, te, err = conn.Client.Insert(shorturl, &cp, &col, Cassandra.ONE)
	if ire != nil {
		log.Println("Invalid request: ", ire.Why)
		num_errors.Add("invalid-request", 1)
		err = os.NewError(ire.String())
		return
	}
	if ue != nil {
		log.Println("Unavailable")
		num_errors.Add("unavailable", 1)
		err = os.NewError(ue.String())
		return
	}
	if te != nil {
		log.Println("Request to database backend timed out")
		num_errors.Add("timeout", 1)
		err = os.NewError(te.String())
		return
	}
	if err != nil {
		log.Println("Generic error: ", err)
		num_errors.Add("os-error", 1)
		err = os.NewError(err.String())
		return
	}
	return "/" + shorturl, nil
}
