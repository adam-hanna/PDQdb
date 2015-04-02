package query

import (
	"encoding/json"
	// "fmt"
	"github.com/adam-hanna/PDQdb/data"
	error_ "github.com/adam-hanna/PDQdb/error"
	"github.com/adam-hanna/PDQdb/index"
	"github.com/adam-hanna/arrayOperations"
	"log"
	"net/http"
)

// this struct holds all of the possible query attributes
type queryStruct struct {
	SELECT  []string               `json:"SELECT"`
	COUNT   string                 `json:"COUNT"`
	WHERE   map[string]interface{} `json:"WHERE"`
	GROUPBY string                 `json:"GROUP BY"`
}

func QueryDB(req *http.Request) interface{} {
	// get a variable ready to hold the incoming query
	var query queryStruct

	// read the incoming html post body and hold it in the query var made above
	decoder := json.NewDecoder(req.Body)

	err := decoder.Decode(&query)
	if err != nil {
		log.Panic(err)
	}

	// what type of query is this, an aggregation or is data being returned?
	switch {
	case len(query.SELECT) == 0 && query.COUNT != "":
		// they want a count!
		return countQuery(query)

	case len(query.SELECT) > 0 && query.COUNT == "":
		// they want some data returned!
		return selectQuery(query)

	default:
		log.Panic(error_.New("Not a valid query! Valid queries must have one of SELECT / COUNT!"))
	}

	// should never get here!
	// NOTE(@adam-hanna): need to implement proper error handling!
	return nil
}

// NOTE(@adam-hanna): do error handling!
func countQuery(query queryStruct) map[string]int {
	// make a map to hold the return
	mReturn := make(map[string]int)
	// make a slice to hold the idx's that match the query
	tempKeysMatched := make([][]uint64, 0)

	// is a group by present?
	switch query.GROUPBY {
	case "":
		// nope, no group by
		// loop through the "WHERE" key/vals. No logical operator (i.e. and / or) means "and" (i.e. intersect not union)
		// write the matching keys to our multi-dimensional array
		// NOTE(@adam-hanna): what if a field in the where is an ID or not an indexed field?
		// NOTE(@adam-hanna): this should be a private function. It will be used many times.
		evalWhereClause(query.WHERE, &tempKeysMatched)

		// find the intersection of the keys
		finalKeysMatched := arrayOperations.SortedIntersectUint64Arr(tempKeysMatched)

		// write to the output map
		mReturn["COUNT"] = len(finalKeysMatched)

	default:
		// yup, there's a group by!
		// first, grab the unique vals and their locations in the data set of the group-by col
		groupByIndex := index.GetIndexByColName(query.GROUPBY)

		// Next, loop through the "WHERE" key/vals. No logical operator (i.e. and / or) means "and" (i.e. intersect not union)
		// write the matching keys to our multi-dimensional array
		// NOTE(@adam-hanna): what if a field in the where is an ID or not an indexed field?
		// NOTE(@adam-hanna): this should be a private function. It will be used many times.
		evalWhereClause(query.WHERE, &tempKeysMatched)

		// find the intersection of the where keys
		whereIntersect := arrayOperations.SortedIntersectUint64Arr(tempKeysMatched)

		// lastly, find the intersection of the where and groupby idx's
		for key, val := range groupByIndex {
			// find the intersection
			mReturn[key] = len(arrayOperations.SortedIntersectUint64(whereIntersect, val))
		}
	}

	return mReturn
}

func selectQuery(query queryStruct) []map[string]interface{} {
	// make an array to hold the return
	var aReturn []map[string]interface{}
	// make a slice to hold the idx's that match the query
	tempKeysMatched := make([][]uint64, 0)

	// is a group by present?
	switch query.GROUPBY {
	case "":
		// no groupby present. good. they aren't supported in select queries
		// Loop through the "WHERE" key/vals. No logical operator (i.e. and / or) means "and" (i.e. intersect not union)
		// write the matching keys to our multi-dimensional array
		// NOTE(@adam-hanna): what if a field in the where is an ID or not an indexed field?
		// NOTE(@adam-hanna): this should be a private function. It will be used many times.
		evalWhereClause(query.WHERE, &tempKeysMatched)

		// find the intersection of the idx's
		finalKeysMatched := arrayOperations.SortedIntersectUint64Arr(tempKeysMatched)

		// redimension the return array
		aReturn = make([]map[string]interface{}, len(finalKeysMatched))

		// loop through the idx's that match all of our where clauses, pulling the data
		for matchedIdx := range finalKeysMatched {
			// make a map that represents the data to be returned
			// the keys of this map represent the column names, the vals represent the data points
			aReturn[matchedIdx] = make(map[string]interface{})

			// Finally, grab the data that the user has asked to be returned
			// NOTE(@adam-hanna): add support for "*"
			for idx := range query.SELECT {
				aReturn[matchedIdx][query.SELECT[idx]] = data.GetDataPointByIdx(query.SELECT[idx], finalKeysMatched[matchedIdx])
			}
		}

	default:
		// groupby's are not allowed in select queries!
		log.Panic(error_.New("Not a valid query! GROUPBY parameters are not allowed in SELECT queries!"))
	}

	return aReturn
}

func evalWhereClause(oWhere map[string]interface{}, aIdxsMatched *[][]uint64) {
	for key, val := range oWhere {
		switch key {
		case "$OR":
			*aIdxsMatched = append(*aIdxsMatched, evalOrClause(val.([]interface{})))
		default:
			*aIdxsMatched = append(*aIdxsMatched, index.QueryIndex(key, val.(string)))
		}
	}
}

func evalOrClause(orVal []interface{}) []uint64 {
	aTemp := make([][]uint64, 0)

	for idx := range orVal {
		for key, val := range orVal[idx].(map[string]interface{}) {
			switch key {
			case "$OR":
				aTemp = append(aTemp, evalOrClause(val.([]interface{})))
			default:
				aTemp = append(aTemp, index.QueryIndex(key, val.(string)))
			}
		}
	}

	return arrayOperations.UnionUint64Arr(aTemp)
}
