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
	"compress/gzip"
	"encoding/json"
	// "fmt"
	"github.com/adam-hanna/PDQdb/globals"
	"github.com/adam-hanna/PDQdb/index"
	"io"
	"log"
	"os"
	"strconv"
)

type configJsonDescriptorStruct struct {
	Header      []interface{} `json:"header"`
	IdField     string        `json:"id_field"`
	IndexFields []string      `json:"index_fields"`
	StartAtLine uint          `json:"start_at_line"`
}

func LoadAndTransformCsvData(cliFlags globals.CliFlagsStruct) {
	// Open the JSON config file.
	csvConfigFileHandle, err := os.Open(cliFlags.ConfigFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer csvConfigFileHandle.Close()
	// Open the CSV file.
	csvFileHandle, err := os.Open(cliFlags.FilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer csvFileHandle.Close()
	// Get ready to start decoding the JSON config file.
	csvConfigFileJsonDecoder := json.NewDecoder(csvConfigFileHandle)
	var configJsonDescriptor configJsonDescriptorStruct
	err = csvConfigFileJsonDecoder.Decode(&configJsonDescriptor)
	if err != nil {
		log.Fatal(err)
	}
	// fmt.Println(configJsonDescriptor)
	// initialize the index map
	if len(configJsonDescriptor.IndexFields) > 0 {
		index.InitializeIndexes(configJsonDescriptor.IndexFields)
	}

	// Get ready to start reading the CSV file.
	csvFileReader := csv.NewReader(csvFileHandle)
	var csvFileLineCount uint = 1

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

		// read the data and create a map
		recordMap := map[string]interface{}
		for idx, dataRecordFieldVal := range dataRecord {
			var dataRecordFieldName string
			var dataRecordFieldTypeString string
			// There should always be only one iteration of this loop.
			for key, val := range configJsonDescriptor.Header[idx].(map[string]interface{}) {
				dataRecordFieldName = key
				dataRecordFieldTypeString = val.(string)
			}
			if dataRecordFieldVal != "" {
				recordMap[dataRecordFieldName] = dataRecordFieldVal
				
			} else {
				recordMap[dataRecordFieldName] = nil
			}
		}

		// marshal the map to json
		jsonDataRecordBytes, err := json.Marshal(recordMap)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			log.Print(err)
		}

		// compress the json with gzip
		var b bytes.Buffer
		gz := gzip.NewWriter(&b)
		if _, err := gz.Write(jsonDataRecordBytes); err != nil {
		    panic(err)
		}
		if err := gz.Flush(); err != nil {
		    panic(err)
		}
		if err := gz.Close(); err != nil {
		    panic(err)
		}

		// add the record to the map
		// Assumes the data set's key is always a string.
		globals.DataSet[recordMap[configJsonDescriptor.IdField].(string)] = b
		
		// add the necessary indexes
		if len(configJsonDescriptor.IndexFields) > 0 {
			index.AppendIndex(configJsonDescriptor.IndexFields, recordMap[configJsonDescriptor.IdField].(string), recordMap)
		}

		// next row of the csv
		csvFileLineCount += 1
	}
	// fmt.Print(globals.DataSet)
}
