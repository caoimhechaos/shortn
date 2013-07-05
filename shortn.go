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

package main

import (
	"ancientsolutions.com/ancientauth"
	"ancientsolutions.com/doozer/exportedservice"
	"expvar"
	"flag"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"text/template"
)

var store *CassandraStore
var num_requests *expvar.Int = expvar.NewInt("num-requests")
var num_views *expvar.Int = expvar.NewInt("num-views")
var num_edits *expvar.Int = expvar.NewInt("num-edits")
var num_redirects *expvar.Int = expvar.NewInt("num-redirects")
var num_notfounds *expvar.Int = expvar.NewInt("num-notfounds")

var fmap = template.FuncMap{
	"html": template.HTMLEscaper,
	"url":  UserInputFormatter,
}
var addurl_templ *template.Template
var done_templ *template.Template
var error_templ *template.Template
var fourohfour_templ *template.Template
var authenticator *ancientauth.Authenticator

func UserInputFormatter(v ...interface{}) string {
	return template.HTMLEscapeString(url.QueryEscape(v[0].(string)))
}

func Shortn(w http.ResponseWriter, req *http.Request) {
	var shorturl string = strings.Split(req.URL.Path, "/")[1]
	var templ_vars = make(map[string]string)
	var err error

	num_requests.Add(1)

	if shorturl == "" {
		/* People need to be logged in in order to add URLs. */
		var user string = authenticator.GetAuthenticatedUser(req)

		// TODO(tonnerre): Count errors properly here.
		if user == "" {
			authenticator.RequestAuthorization(w, req)
			return
		}

		templ_vars["user"] = user

		err = req.ParseForm()
		if err != nil {
			error_templ.Execute(w, err.Error())
			return
		}

		if req.FormValue("urltoadd") != "" {
			var newurl *url.URL
			newurl, err = url.Parse(req.URL.String())
			if err != nil {
				error_templ.Execute(w, err.Error())
				return
			}

			if req.TLS != nil {
				newurl.Scheme = "https"
			} else {
				newurl.Scheme = "http"
			}

			newurl.Host = req.Host
			newurl.Path, err =
				store.AddURL(req.FormValue("urltoadd"), user)
			if err != nil {
				error_templ.Execute(w, err.Error())
				return
			}
			num_edits.Add(1)
			templ_vars["url"] = newurl.String()
			done_templ.Execute(w, templ_vars)
			return
		}

		addurl_templ.Execute(w, templ_vars)
	} else {
		var dest string = store.LookupURL(shorturl)

		num_views.Add(1)

		if dest == "" {
			w.WriteHeader(http.StatusNotFound)
			fourohfour_templ.Execute(w, shorturl)
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
	var cassandra_server, corpus string
	var ca, pub, priv, authserver string
	var bindto, templatedir, servicename string
	var doozer_uri, doozer_buri string
	var exporter *exportedservice.ServiceExporter
	var err error

	flag.BoolVar(&help, "help", false, "Display help")
	flag.StringVar(&bindto, "bind", "[::]:80",
		"The address to bind the web server to")
	flag.StringVar(&cassandra_server, "cassandra-server", "localhost:9160",
		"The Cassandra database server to use")
	flag.StringVar(&corpus, "corpus", "links",
		"The column family containing the short links for this service")
	flag.StringVar(&ca, "cacert", "cacert.pem",
		"Path to the X.509 certificate of the certificate authority")
	flag.StringVar(&pub, "cert", "shortn.pem",
		"Path to the X.509 certificate")
	flag.StringVar(&priv, "key", "shortn.key",
		"Path to the X.509 private key file")
	flag.StringVar(&templatedir, "template-dir", "/var/www/templates",
		"Path to the HTML templates for the web interface")
	flag.StringVar(&authserver, "auth-server",
		"login.ancient-solutions.com",
		"The server to send the user to")
	flag.StringVar(&doozer_uri, "doozer-uri", os.Getenv("DOOZER_URI"),
		"Doozer URI to connect to")
	flag.StringVar(&doozer_buri, "doozer-boot-uri",
		os.Getenv("DOOZER_BOOT_URI"),
		"Doozer Boot URI to find named clusters")
	flag.StringVar(&servicename, "exported-name", "",
		"Name to export the service as in Doozer")
	flag.Parse()

	if help {
		flag.Usage()
		os.Exit(1)
	}
	addurl_templ = template.Must(template.ParseFiles(templatedir +
		"/addurl.tmpl"))
	addurl_templ.Funcs(fmap)
	done_templ = template.Must(template.ParseFiles(templatedir +
		"/added.tmpl"))
	done_templ.Funcs(fmap)
	error_templ = template.Must(template.ParseFiles(templatedir +
		"/error.tmpl"))
	error_templ.Funcs(fmap)
	fourohfour_templ = template.Must(template.ParseFiles(templatedir +
		"/notfound.tmpl"))
	fourohfour_templ.Funcs(fmap)

	authenticator, err = ancientauth.NewAuthenticator("URL Shortener", pub,
		priv, ca, authserver)
	if err != nil {
		log.Fatal("NewAuthenticator: ", err)
	}

	if store = NewCassandraStore(cassandra_server, corpus); store == nil {
		os.Exit(2)
	}

	http.Handle("/", http.HandlerFunc(Shortn))

	if len(servicename) > 0 {
		exporter, err = exportedservice.NewExporter(doozer_uri,
			doozer_buri)
		if err != nil {
			log.Fatal("NewExporter: ", err)
		}

		err = exporter.ListenAndServeNamedHTTP(servicename, bindto,
			nil)
		if err != nil {
			log.Fatal("ListenAndServeNamedHTTP: ", err)
		}
	} else {
		err = http.ListenAndServe(bindto, nil)
		if err != nil {
			log.Fatal("ListenAndServe: ", err)
		}
	}
}
