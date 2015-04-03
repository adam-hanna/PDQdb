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
	"sort"
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

	// is a group by present?
	switch query.GROUPBY {
	case "":
		// nope, no group by
		// loop through the "WHERE" key/vals. No logical operator (i.e. and / or) means "and" (i.e. intersect not union)
		// write the matching keys to our multi-dimensional array
		// NOTE(@adam-hanna): what if a field in the where is an ID or not an indexed field?
		// write to the output map
		mReturn["COUNT"] = len(evalWhereClause(query.WHERE))

	default:
		// yup, there's a group by!
		// first, grab the unique vals and their locations in the data set of the group-by col
		groupByIndex := index.GetIndexByColName(query.GROUPBY)

		// Next, loop through the "WHERE" key/vals. No logical operator (i.e. and / or) means "and" (i.e. intersect not union)
		// write the matching keys to our multi-dimensional array
		// NOTE(@adam-hanna): what if a field in the where is an ID or not an indexed field?
		// find the intersection of the where keys
		whereIntersect := evalWhereClause(query.WHERE)

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

	// is a group by present?
	switch query.GROUPBY {
	case "":
		// no groupby present. good. they aren't supported in select queries
		// Loop through the "WHERE" key/vals. No logical operator (i.e. and / or) means "and" (i.e. intersect not union)
		// write the matching keys to our multi-dimensional array
		// NOTE(@adam-hanna): what if a field in the where is an ID or not an indexed field?
		// find the intersection of the idx's
		// NOTE(@adam-hanna): should be doing a check on query object in separate function
		// to be sure it meets specs, rather than doing it here?
		finalKeysMatched := evalWhereClause(query.WHERE)

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
		// NOTE(@adam-hanna): should be doing a check on query object in separate function
		// to be sure it meets specs, rather than doing it here?
		log.Panic(error_.New("Not a valid query! GROUPBY parameters are not allowed in SELECT queries!"))
	}

	return aReturn
}

func evalWhereClause(mWhere map[string]interface{}) []uint64 {
	// create an array to hold the idx's of all the data that meet our search
	tempKeysMatched := make([][]uint64, 0)

	// loop through the where clause, applying the appropriate logic where necessary
	for key, val := range mWhere {
		switch key {
		case "$OR":
			tempKeysMatched = append(tempKeysMatched, evalOrClause(val.([]interface{})))
		case "$NOT":
			tempKeysMatched = append(tempKeysMatched, evalNotClause(val.(map[string]interface{})))
		case "$NOR":
			tempKeysMatched = append(tempKeysMatched, evalNorClause(val.([]interface{})))
		default:
			// the default is treated as an $AND, so add it to the temp index
			// we will run an intersection on the temp index last.
			tempKeysMatched = append(tempKeysMatched, index.QueryIndex(key, val.(string)))
		}
	}

	// assume that where the user didn't input a logical operator, that an $AND was implied.
	// therefore, run the interesection...
	return arrayOperations.SortedIntersectUint64Arr(tempKeysMatched)
}

func evalOrClause(orVal []interface{}) []uint64 {
	aTemp := make([][]uint64, 0)

	for idx := range orVal {
		aTemp = append(aTemp, evalWhereClause(orVal[idx].(map[string]interface{})))
	}

	// find union
	unsortedKeys := arrayOperations.UnionUint64Arr(aTemp)

	// the above may be unsorted, so we need to sort it!
	if !sort.IsSorted(uintArray(unsortedKeys)) {
		// the array is not sorted, sort it!
		sort.Sort(uintArray(unsortedKeys))
	}

	return unsortedKeys
}

func notHelper(idxs []uint64) []uint64 {
	// create a temp array for returned idx's
	aTempReturn := make([]uint64, 0)

	// now, take the inverse of the idx's matched
	// first, find how many records we have
	numRecords := data.CountRecords()

	// create two vals for looping
	i := uint64(0)
	j := uint64(0)
	for ; i < numRecords && j < uint64(len(idxs)); i++ {
		switch i {
		case idxs[j]:
			// this is an idx that we found; don't add it to our return array!
			j++
		default:
			// it's not in our list of idx's; let's add it to our return!
			aTempReturn = append(aTempReturn, i)
		}
	}

	// now, add the remaining idx's
	// the length of aTemp will never be greater than the length of the entire db,
	// so we only have to do this once and not on aTemp
	for ; i < numRecords; i++ {
		aTempReturn = append(aTempReturn, i)
	}

	return aTempReturn
}

func evalNotClause(mNot map[string]interface{}) []uint64 {
	// evaluate the elements within the not clause as per usual
	// and then find the idxs that are NOT those!
	return notHelper(evalWhereClause(mNot))

}

func evalNorClause(norVal []interface{}) []uint64 {
	// a nor is an $OR followed by a $NOT.
	return notHelper(evalOrClause(norVal))

}

// these are some helpers for sorting uint64's
type uintArray []uint64

func (s uintArray) Len() int           { return len(s) }
func (s uintArray) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s uintArray) Less(i, j int) bool { return s[i] < s[j] }
