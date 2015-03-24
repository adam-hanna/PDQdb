package data

import (
// "fmt"
)

// column settings are used to store information about the columns - spcifically name, type and
// index, i.e. the order in which they are encountered in the csv.
type columnSettings struct {
	colType  string
	colIndex uint64 // no way there's this many columns, but better safe than sorry
	bIdCol   bool
}

var cols map[string]columnSettings // the map key is the column name

// this represents the database to be stored
// the parent array are columns, the arrays are the data elems i.e. rows
// NOTE(@adam-hanna): is a map a better way of implementing this?
var dataSet [][]interface{}

// make the primary key map
// assumes primary key is a string
// NOTE(@adam-hanna): change this to accept different types
var primeKeyDictionary map[string]uint64

func InitializeDataset() {
	primeKeyDictionary = make(map[string]uint64)
}

func InitializeColumnSettings(columns []interface{}, idField string) {
	dataSet = make([][]interface{}, len(columns))
	cols = make(map[string]columnSettings)

	for idx := range columns {
		for key, val := range columns[idx].(map[string]interface{}) {
			fieldName := key
			fieldTypeString := val.(string)

			// set the values to the column map
			cols[fieldName] = columnSettings{fieldTypeString, uint64(idx), fieldName == idField}

			// initalize the column
			// NOTE(@adam-hanna): is this necessary?
			dataSet[idx] = make([]interface{}, 0)
		}
	}
}

func GetFullRowOfDataByKey(key string) map[string]interface{} {
	return GetFullRowOfDataByIdx(primeKeyDictionary[key])
}

func GetFullRowOfDataByIdx(idx uint64) map[string]interface{} {
	temp := make(map[string]interface{})

	for key, val := range cols {
		temp[key] = dataSet[val.colIndex][idx]
	}

	return temp
}

func GetDataPointByIdx(colName string, idx uint64) interface{} {
	return dataSet[GetColIndexByName(colName)][idx]
}

func SetData(newData []interface{}) {
	idFieldIdx := GetIdFieldIdx()

	for key := range newData {
		dataSet[key] = append(dataSet[key], newData[key])
	}

	// add the location to the primeKey dictionary
	// NOTE(@adam-hanna): check for dupe ids?
	primeKeyDictionary[newData[idFieldIdx].(string)] = uint64(len(dataSet[0]) - 1)
}

func GetColTypeByName(colName string) string {
	return cols[colName].colType
}

func GetColIndexByName(colName string) uint64 {
	return cols[colName].colIndex
}

func GetIdFieldName() string {
	for key, val := range cols {
		if val.bIdCol {
			return key
		}
	}

	// NOTE(@adam-hanna): fix this! but don't want to return two vals (i.e. error)?
	return "not found"
}

func GetIdFieldIdx() uint64 {
	for _, val := range cols {
		if val.bIdCol {
			return val.colIndex
		}
	}

	// NOTE(@adam-hanna): fix this! but don't want to return two vals (i.e. error)?
	return uint64(0)
}

func CountRecords() uint64 {
	return uint64(len(dataSet[0]))
}
