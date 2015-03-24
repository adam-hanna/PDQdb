package index

// this stores all of the indexes. The parent map has keys which are
// the names of the indexed columns. The imbedded map has keys which are
// the unique values within that column (unique values from the data).
// This imbedded map then stores an array of the Dataset keys
// that have that value

import (
	// "fmt"
	"github.com/adam-hanna/PDQdb/data"
)

var indexes map[string]map[string][]string

func InitializeIndexes(indexFields []string) {
	// NOTE(@adam-hanna): use types?
	indexes = make(map[string]map[string][]string)

	// loop through the provided aray of index fields, adding them to our index map
	for fieldName := range indexFields {
		indexes[indexFields[fieldName]] = make(map[string][]string)
	}
}

func AppendIndex(indexFieldNames []string, id string, record []interface{}) {
	for fieldName := range indexFieldNames {
		indexes[indexFieldNames[fieldName]][record[data.GetColIndexByName(indexFieldNames[fieldName])].(string)] = append(indexes[indexFieldNames[fieldName]][record[data.GetColIndexByName(indexFieldNames[fieldName])].(string)], record[data.GetColIndexByName(id)].(string))
	}
}

func QueryIndex(query map[string]interface{}) []interface{} {
	temp := make([]interface{}, 0)

	// grab the keys from the index that match the query
	// NOTE(@adam-hanna):
	// Need to check for duplicate keys!
	var aKeys []string
	for key, val := range query {
		aKeys = append(indexes[key][val.(string)])
	}

	// now grab the data
	for key := range aKeys {
		temp = append(temp, data.GetFullRowOfDataByKey(aKeys[key]))
	}

	return temp
}
