package index

// this stores all of the indexes. The parent map has keys which are
// the names of the indexed columns. The imbedded map has keys which are
// the unique values within that column (unique values from the data).
// This imbedded map then stores an array of the Dataset keys
// that have that value

import (
	// "fmt"
	"github.com/adam-hanna/PDQdb/data"
	"sort"
)

// This map holds all of the indexes. The first map keys are the indexed column names.
// The nested map keys are the unique values of that column. Only supports strings for now!
// The nested map values are the corresponding locations (e.g. idx) in the dataset that
// the data lives at. The idx's are stored in sorted order to allow faster searching.
// One important thing to note, is that due to how we are adding data to the index, they will
// always be sorted in ascending order!
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

// sort the idx's of the all the indexes
func SortIndexes() {
	for _, val1 := range indexes {
		for _, val2 := range val1 {
			sort.Sort(uintArray(val2))
		}
	}
}

// sort the idx's of index by colName
func SortIndexbyColName(colName string) {
	for _, val := range indexes[colName] {
		sort.Sort(uintArray(val))
	}
}

// sort the idx's of an index by colName and unique val
// only support string unique val's for now!
func SortIndexbyColNameAndVal(colName string, uniqueVal string) {
	sort.Sort(uintArray(indexes[colName][uniqueVal]))
}

// this function returns the idx's that match a query
func QueryIndex(queryKey string, queryVal string) []uint64 {
	// grab all the idx's from the index that match the query
	// the query key is the name of the indexed column,
	// the queryVal is the val that we're looking for. We only support indexes on string
	// fields for now
	return indexes[queryKey][queryVal]

}

// this function returns all the indexes
func GetIndexes() map[string]map[string][]uint64 {
	return indexes
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

// Need to create some helpers for our sort.Sort
type uintArray []uint64

func (s uintArray) Len() int           { return len(s) }
func (s uintArray) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s uintArray) Less(i, j int) bool { return s[i] < s[j] }
