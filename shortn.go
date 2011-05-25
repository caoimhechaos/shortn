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
	"expvar"
	"flag"
	"http"
	"io"
	"log"
	"os"
	"strings"
	"template"
)

var store *CassandraStore
var num_requests *expvar.Int = expvar.NewInt("num-requests")
var num_redirects *expvar.Int = expvar.NewInt("num-redirects")
var num_notfounds *expvar.Int = expvar.NewInt("num-notfounds")

var fmap = template.FormatterMap{
	"html": template.HTMLFormatter,
	"url":  UserInputFormatter,
}
var templ = template.MustParseFile("templates/notfound.tmpl", fmap)

func UserInputFormatter(w io.Writer, fmt string, v ...interface{}) {
	template.HTMLEscape(w, []byte(http.URLEscape(v[0].(string))))
}

func Goto(w http.ResponseWriter, req *http.Request) {
	var shorturl string = strings.Split(req.URL.Path, "/", -1)[1]

	num_requests.Add(1)

	if shorturl == "" {
	} else {
		var dest string = store.LookupURL(shorturl)
		if dest == "" {
			w.WriteHeader(http.StatusNotFound)
			templ.Execute(w, shorturl)
			num_notfounds.Add(1)
		} else {
			w.Header().Add("Location", dest)
			w.WriteHeader(http.StatusFound)
			num_redirects.Add(1)
		}
	}
}

func main() {
	var help bool
	var cassandra_server string
	var bindto string
	var corpus string

	flag.BoolVar(&help, "help", false, "Display help")
	flag.StringVar(&bindto, "bind", "0.0.0.0:80",
		"The address to bind the web server to")
	flag.StringVar(&cassandra_server, "cassandra-server", "localhost:9160",
		"The Cassandra database server to use")
	flag.StringVar(&corpus, "corpus", "links",
		"The column family containing the short links for this service")
	flag.Parse()

	if help {
		flag.Usage()
		os.Exit(1)
	}

	if store = NewCassandraStore(cassandra_server, corpus); store == nil {
		os.Exit(2)
	}

	http.Handle("/", http.HandlerFunc(Goto))

	err := http.ListenAndServe(bindto, nil)
	if err != nil {
		log.Fatal("ListenAndServe:", err)
	}
}
