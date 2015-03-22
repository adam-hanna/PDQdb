package index

// this stores all of the indexes. The parent map has keys which are
// the names of the indexed columns. The imbedded map has keys which are
// the unique values within that column (unique values from the data).
// This imbedded map then stores an array of the Dataset keys
// that have that value

import (
	"encoding/json"
	// "fmt"
	"compress/gzip"
	"github.com/adam-hanna/PDQdb/globals"
	"log"
)

var indexes map[string]map[string][]string

func InitializeIndexes(indexFields []string) {
	indexes = make(map[string]map[string][]string)

	// loop through the provided aray of index fields, adding them to our index map
	for fieldName := range indexFields {
		indexes[indexFields[fieldName]] = make(map[string][]string)
	}
}

func AppendIndex(indexFields []string, id string, record map[string]interface{}) {
	for fieldName := range indexFields {
		indexes[indexFields[fieldName]][record[indexFields[fieldName]].(string)] = append(indexes[indexFields[fieldName]][record[indexFields[fieldName]].(string)], id)
	}
}

func QueryIndex(query map[string]interface{}) []byte {
	// grab the keys from the index that match the query
	// NOTE(@adam-hanna):
	// Need to check for duplicate keys!
	var aKeys []string
	for key, val := range query {
		aKeys = append(indexes[key][val.(string)])
	}

	// now grab the data. Remember, they're in BSON bytes!
	aReturn := make([]interface{}, len(aKeys))
	for keys := range aKeys {
		// uncompress the data
		var b bytes.Buffer
		b = globals.DataSet[aKeys[keys]]
		r, err := gzip.NewReader(&b)
		io.Copy(os.Stdout, r)
		r.Close()

		// write the uncompressed data to the map
		aReturn[keys] = b

	}

	return aReturn
}
