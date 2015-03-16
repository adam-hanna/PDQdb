package server

import (
	"github.com/adam-hanna/PDQdb/data"
	"net/http"
	"strconv"
)

func StartServer(hostname string, port uint16) error {
	http.HandleFunc("/", serveMainRoute)

	var host string = hostname + strconv.Itoa(int(port))

	return http.ListenAndServe(host, nil)
}

func serveMainRoute(res http.ResponseWriter, req *http.Request) {
	// Read the request and respond accordingly
	if req.Method == "GET" {
		// extract the key from the header of the request
		var key string = req.Header.Get("key")

		//grab the key that the user is looking for
		var val string = data.DataSet[key]

		// write the headers
		res.WriteHeader(http.StatusOK)
		res.Header().Set("Content-Type", "text/plain")

		// send back the response
		res.Write([]byte(val))
	} else {
		// write the headers
		res.WriteHeader(http.StatusOK)
		res.Header().Set("Content-Type", "text/plain")

		// send back the response
		res.Write([]byte("Pretty Damn Quick!\n"))
	}
}
