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

// This map holds all of the indexes. The first map keys are the indexed columns.
// The nested map keys are the unique values of that column. Only supports strings for now!
// The nested map values are the corresponding locations (e.g. idx) in the dataset that
// the data lives at
var indexes map[string]map[string][]uint64

// create the index map. This is only done once on startup after reading the config.json
func InitializeIndexes(indexFields []string) {
	// NOTE(@adam-hanna): use types, not just strings!
	indexes = make(map[string]map[string][]uint64)

	// loop through the provided aray of index fields, adding them to our index map
	for fieldName := range indexFields {
		indexes[indexFields[fieldName]] = make(map[string][]uint64)
	}
}

// add a new data record to be indexed.
func AppendIndex(indexFieldNames []string, id string, record []interface{}) {
	for fieldName := range indexFieldNames {
		indexes[indexFieldNames[fieldName]][record[data.GetColIndexByName(indexFieldNames[fieldName])].(string)] = append(indexes[indexFieldNames[fieldName]][record[data.GetColIndexByName(indexFieldNames[fieldName])].(string)], data.GetRecordIdxByKey(record[data.GetColIndexByName(id)].(string)))
	}
}

// this function returns the idx's that match a query
func QueryIndex(queryKey string, queryVal string) []uint64 {
	// grab all the idx's from the index that match the query
	// the query key is the name of the indexed column,
	// the queryVal is the val that we're looking for. We only support indexes on string
	// fields for now
	return indexes[queryKey][queryVal]

}

// this function returns all of the unique values and their locations in the data of a column
// only supports strings for now!
func GetIndexByColName(colName string) map[string][]uint64 {
	return indexes[colName]
}

// this function returns all of the unique values of a column
// only supports strings for now!
func GetUniqueValsByColName(colName string) []string {
	tempArr := make([]string, 0)

	for key := range indexes[colName] {
		tempArr = append(tempArr, key)
	}

	return tempArr
}
