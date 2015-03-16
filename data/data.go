/*
 * Copyright (C) 2015-present Adam Hanna <ahanna@alumni.mines.edu>
 * Copyright (C) 2015-present Jonathan Barronville <jonathan@belairlabs.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package data

import (
	"encoding/csv"
	// "encoding/json"
	// "fmt"
	"gopkg.in/mgo.v2/bson"
	"io"
	"log"
	"os"
	"strconv"
)

type ConfigJsonDescriptor struct {
	Header      []interface{} `json:"header"`
	IndexField  string        `json:"index_field"`
	StartAtLine uint          `json:"start_at_line"`
}

var DataSet map[string][]byte

func LoadAndTransformCsvData(csvFileHandle *os.File, configJsonDescriptor *ConfigJsonDescriptor) {
	// Get ready to start reading the CSV file.
	csvFileReader := csv.NewReader(csvFileHandle)
	var csvFileLineCount uint = 1
	DataSet = make(map[string][]byte)
	for {
		dataRecord, err := csvFileReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Print(err)
		}
		if configJsonDescriptor.StartAtLine > csvFileLineCount {
			csvFileLineCount += 1
			continue
		}
		// NOTE(@jonathanmarvens):
		// The code you're about to see is probably super inefficient.
		// You write bad code every now and then, too, so don't judge me.
		// TODO(@jonathanmarvens): Fucking fix this shit ASAP.
		bsonDataRecordMap := make(bson.M)
		for idx, dataRecordFieldVal := range dataRecord {
			var dataRecordFieldName string
			var dataRecordFieldTypeString string
			// There should always be only one iteration of this loop.
			for key, val := range configJsonDescriptor.Header[idx].(map[string]interface{}) {
				dataRecordFieldName = key
				dataRecordFieldTypeString = val.(string)
			}
			switch dataRecordFieldTypeString {
			case "string":
				bsonDataRecordMap[dataRecordFieldName] = dataRecordFieldVal
			case "bool":
				dataRecordFieldTypeBoolVal, err := strconv.ParseBool(dataRecordFieldVal)
				if err != nil {
					// NOTE(@jonathanmarvens): Should be Fatal?
					log.Print(err)
				}
				bsonDataRecordMap[dataRecordFieldName] = dataRecordFieldTypeBoolVal
			case "double":
				dataRecordFieldTypeDoubleVal, err := strconv.ParseFloat(dataRecordFieldVal, 64)
				if err != nil {
					// NOTE(@jonathanmarvens): Should be Fatal?
					log.Print(err)
				}
				bsonDataRecordMap[dataRecordFieldName] = dataRecordFieldTypeDoubleVal
			case "int32":
				dataRecordFieldTypeInt32ValTmp, err := strconv.ParseInt(dataRecordFieldVal, 10, 32)
				if err != nil {
					// NOTE(@jonathanmarvens): Should be Fatal?
					log.Print(err)
				}
				dataRecordFieldTypeInt32Val := int32(dataRecordFieldTypeInt32ValTmp)
				bsonDataRecordMap[dataRecordFieldName] = dataRecordFieldTypeInt32Val
			case "int64":
				dataRecordFieldTypeInt64Val, err := strconv.ParseInt(dataRecordFieldVal, 10, 64)
				if err != nil {
					// NOTE(@jonathanmarvens): Should be Fatal?
					log.Print(err)
				}
				bsonDataRecordMap[dataRecordFieldName] = dataRecordFieldTypeInt64Val
			}
		}
		// fmt.Println(bsonDataRecordMap)
		// for key, val := range bsonDataRecordMap {
		// 	fmt.Printf("%s: %v\n", key, val)
		// 	fmt.Print("\n\n")
		// }
		bsonDataRecordBytes, err := bson.Marshal(bsonDataRecordMap)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			log.Print(err)
		}

		// // NOTE(@jonathanmarvens):
		// // Listen, I know there are better ways to test.
		// // This works for now, so stop judging me!!!
		// jsonEncodedBytesFromBson, err := json.Marshal(&bsonDataRecordMap)
		// if err != nil {
		// 	log.Print(err)
		// }
		// os.Stdout.Write(jsonEncodedBytesFromBson)
		// fmt.Print("\n")

		// Assumes the data set's key is always a string.
		DataSet[bsonDataRecordMap[configJsonDescriptor.IndexField].(string)] = bsonDataRecordBytes
		// fmt.Printf("%s: %v\n", bsonDataRecordMap[configJsonDescriptor.IndexField].(string), data.DataSet[bsonDataRecordMap[configJsonDescriptor.IndexField].(string)])
		csvFileLineCount += 1
	}
	// fmt.Print(data.DataSet)
}
