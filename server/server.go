package server

import (
	"github.com/adam-hanna/PDQdb/data"
	"github.com/gorilla/mux"
	"net/http"
	"strconv"
)

func StartServer(hostname string, port uint16) error {
	mx := mux.NewRouter()
	mx.HandleFunc("/key/{key}", ProcessKey)
	mx.HandleFunc("/", serveMainRoute)

	var host string = hostname + strconv.Itoa(int(port))

	return http.ListenAndServe(host, mx)

}

func serveMainRoute(res http.ResponseWriter, req *http.Request) {
	// write the headers
	res.WriteHeader(http.StatusOK)
	res.Header().Set("Content-Type", "text/plain")

	// send back the response
	res.Write([]byte("Pretty Damn Quick!\n"))
}

func ProcessKey(res http.ResponseWriter, req *http.Request) {
	if req.Method == "GET" {
		// extract the key from the url of the request
		vars := mux.Vars(req)
		key := vars["name"]

		//grab the key that the user is looking for
		var val []byte = data.DataSet[key]

		// write the headers
		res.WriteHeader(http.StatusOK)
		res.Header().Set("Content-Type", "text/plain")

		// send back the response
		res.Write(val)
	} else {
		// do POST / PUT stuff

		// write the headers
		res.WriteHeader(http.StatusOK)
		res.Header().Set("Content-Type", "text/plain")

		// send back the response
		res.Write([]byte("Pretty Damn Quick!\n"))
	}
}
