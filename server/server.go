/*
 * Copyright (C) 2015-present Adam Hanna <ahanna@alumni.mines.edu>
 * Copyright (C) 2015-present Jonathan Barronville <jonathan@belairlabs.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

// NOTE(@adam-hanna):
// you're being lazy and aren't properly handleing errors and
// subsequently sending http error codes!

package server

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	// "fmt"
	"github.com/adam-hanna/PDQdb/globals"
	"github.com/adam-hanna/PDQdb/index"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
	// "os"
	"strconv"
)

func StartServer(hostname string, port uint16) error {
	mx := mux.NewRouter()
	mx.HandleFunc("/key/{key}", processKey)
	mx.HandleFunc("/count", countKeys)
	mx.HandleFunc("/query", queryRoute)
	mx.HandleFunc("/", serveMainRoute)

	var host string = hostname + ":" + strconv.Itoa(int(port))

	log.Printf("Listening on: %s:%d", hostname, port)

	return http.ListenAndServe(host, mx)

}

func serveMainRoute(res http.ResponseWriter, req *http.Request) {
	// write the headers
	res.Header().Set("Content-Type", "text/plain")

	// send back the response
	res.Write([]byte("Pretty Damn Quick!\n"))
	res.WriteHeader(http.StatusOK)
}

func processKey(res http.ResponseWriter, req *http.Request) {
	if req.Method == "GET" {
		// extract the key from the url of the request
		vars := mux.Vars(req)
		key := vars["key"]

		//grab the key that the user is looking for
		b := bytes.NewBuffer(globals.DataSet[key])

		// uncompress the data
		r, err := gzip.NewReader(b)
		if err != nil {
			panic(err)
		}

		temp, err := ioutil.ReadAll(r)
		if err != nil {
			panic(err)
		}
		r.Close()

		var b1 bytes.Buffer
		_, err = b1.Write(temp)
		if err != nil {
			panic(err)
		}

		// write the headers
		res.Header().Set("Content-Type", "application/json")

		// send back the response
		res.Write(b1.Bytes())
		res.WriteHeader(http.StatusOK)
	} else {
		// do POST / PUT / DELETE stuff

		// write the headers
		res.Header().Set("Content-Type", "text/plain")

		// send back the response
		res.Write([]byte("Pretty Damn Quick!\n"))
		res.WriteHeader(http.StatusOK)
	}
}

func countKeys(res http.ResponseWriter, req *http.Request) {
	if req.Method == "GET" {

		type count struct {
			Count int
		}
		c1 := count{}
		c1.Count = len(globals.DataSet)

		jsonData, err := json.Marshal(c1)
		if err != nil {
			log.Print(err)
		}

		// write the headers
		res.Header().Set("Content-Type", "application/json")

		// send back the response
		res.Write(jsonData)
		res.WriteHeader(http.StatusOK)
	} else {
		// do POST / PUT / DELETE stuff

		// write the headers
		res.Header().Set("Content-Type", "text/plain")

		// send back the response
		res.Write([]byte("Pretty Damn Quick!\n"))
		res.WriteHeader(http.StatusOK)
	}
}

func queryRoute(res http.ResponseWriter, req *http.Request) {
	if req.Method == "POST" {
		if req.Header.Get("Content-Type") == "application/json" {
			decoder := json.NewDecoder(req.Body)

			var template interface{}
			err := decoder.Decode(&template)
			if err != nil {
				panic(err)
			}
			m := template.(map[string]interface{})

			jsonOut, err := json.Marshal(index.QueryIndex(m))
			if err != nil {
				log.Print(err)
			}

			// write the headers
			// change this to json
			res.Header().Set("Content-Type", "application/json")

			// send back the response
			res.Write(jsonOut)
			res.WriteHeader(http.StatusOK)
		}
	} else {
		// do GET / PUT / DELETE stuff

		// write the headers
		res.Header().Set("Content-Type", "text/plain")

		// send back the response
		res.Write([]byte("Pretty Damn Quick!\n"))
		res.WriteHeader(http.StatusOK)
	}
}
