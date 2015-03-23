package index

// this stores all of the indexes. The parent map has keys which are
// the names of the indexed columns. The imbedded map has keys which are
// the unique values within that column (unique values from the data).
// This imbedded map then stores an array of the Dataset keys
// that have that value

import (
	"bytes"
	"compress/gzip"
	// "fmt"
	"encoding/json"
	"github.com/adam-hanna/PDQdb/globals"
	"io/ioutil"
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

func QueryIndex(query map[string]interface{}) []interface{} {
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
		b := bytes.NewBuffer(globals.DataSet[aKeys[keys]])
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

		// unmarshal the json and write to the array
		err = json.Unmarshal(b1.Bytes(), &aReturn[keys])
		if err != nil {
			panic(err)
		}

	}

	return aReturn
}
