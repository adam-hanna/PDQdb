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
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	// "fmt"
	"bytes"
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
		var recordMap map[string]interface{}
		recordMap = make(map[string]interface{})
		for idx, dataRecordFieldVal := range dataRecord {
			var dataRecordFieldName string
			var dataRecordFieldTypeString string

			// There should always be only one iteration of this loop.
			for key, val := range configJsonDescriptor.Header[idx].(map[string]interface{}) {
				dataRecordFieldName = key
				dataRecordFieldTypeString = val.(string)
			}
			if dataRecordFieldVal != "" {
				// convert the input string based on data type
				err = convertStringToType(recordMap, dataRecordFieldName, dataRecordFieldVal, dataRecordFieldTypeString)
				if err != nil {
					log.Print(err)
				}
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
		globals.DataSet[recordMap[configJsonDescriptor.IdField].(string)] = b.Bytes()

		// add the necessary indexes
		if len(configJsonDescriptor.IndexFields) > 0 {
			index.AppendIndex(configJsonDescriptor.IndexFields, recordMap[configJsonDescriptor.IdField].(string), recordMap)
		}

		// next row of the csv
		csvFileLineCount += 1
	}
	// fmt.Print(globals.DataSet)
}

func convertStringToType(recordMap map[string]interface{}, dataRecordFieldName string, dataRecordFieldVal string, dataRecordFieldTypeString string) error {
	switch dataRecordFieldTypeString {
	case "string":
		recordMap[dataRecordFieldName] = dataRecordFieldVal

	case "bool":
		dataRecordFieldTypeBoolVal, err := strconv.ParseBool(dataRecordFieldVal)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		recordMap[dataRecordFieldName] = dataRecordFieldTypeBoolVal
	case "float32":
		dataRecordFieldTypeFloat32ValTmp, err := strconv.ParseFloat(dataRecordFieldVal, 32)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		dataRecordFieldTypeFloat32Val := float32(dataRecordFieldTypeFloat32ValTmp)
		recordMap[dataRecordFieldName] = dataRecordFieldTypeFloat32Val
	case "float64":
		dataRecordFieldTypeFloat64Val, err := strconv.ParseFloat(dataRecordFieldVal, 64)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		recordMap[dataRecordFieldName] = dataRecordFieldTypeFloat64Val
	case "int":
		dataRecordFieldTypeIntValTmp, err := strconv.ParseInt(dataRecordFieldVal, 10, 0)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		dataRecordFieldTypeIntVal := int(dataRecordFieldTypeIntValTmp)
		recordMap[dataRecordFieldName] = dataRecordFieldTypeIntVal
	case "int8":
		dataRecordFieldTypeInt8ValTmp, err := strconv.ParseInt(dataRecordFieldVal, 10, 8)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		dataRecordFieldTypeInt8Val := int8(dataRecordFieldTypeInt8ValTmp)
		recordMap[dataRecordFieldName] = dataRecordFieldTypeInt8Val
	case "int16":
		dataRecordFieldTypeInt16ValTmp, err := strconv.ParseInt(dataRecordFieldVal, 10, 16)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		dataRecordFieldTypeInt16Val := int16(dataRecordFieldTypeInt16ValTmp)
		recordMap[dataRecordFieldName] = dataRecordFieldTypeInt16Val
	case "int32":
		dataRecordFieldTypeInt32ValTmp, err := strconv.ParseInt(dataRecordFieldVal, 10, 32)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		dataRecordFieldTypeInt32Val := int32(dataRecordFieldTypeInt32ValTmp)
		recordMap[dataRecordFieldName] = dataRecordFieldTypeInt32Val
	case "int64":
		dataRecordFieldTypeInt64Val, err := strconv.ParseInt(dataRecordFieldVal, 10, 64)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			log.Print(err)
		}
		recordMap[dataRecordFieldName] = dataRecordFieldTypeInt64Val
	case "uint":
		dataRecordFieldTypeUintValTmp, err := strconv.ParseUint(dataRecordFieldVal, 10, 0)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		dataRecordFieldTypeUintVal := uint(dataRecordFieldTypeUintValTmp)
		recordMap[dataRecordFieldName] = dataRecordFieldTypeUintVal
	case "uint8":
		dataRecordFieldTypeUint8ValTmp, err := strconv.ParseUint(dataRecordFieldVal, 10, 8)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		dataRecordFieldTypeUint8Val := uint8(dataRecordFieldTypeUint8ValTmp)
		recordMap[dataRecordFieldName] = dataRecordFieldTypeUint8Val
	case "uint16":
		dataRecordFieldTypeUint16ValTmp, err := strconv.ParseUint(dataRecordFieldVal, 10, 16)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		dataRecordFieldTypeUint16Val := uint16(dataRecordFieldTypeUint16ValTmp)
		recordMap[dataRecordFieldName] = dataRecordFieldTypeUint16Val
	case "uint32":
		dataRecordFieldTypeUint32ValTmp, err := strconv.ParseUint(dataRecordFieldVal, 10, 32)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		dataRecordFieldTypeUint32Val := uint32(dataRecordFieldTypeUint32ValTmp)
		recordMap[dataRecordFieldName] = dataRecordFieldTypeUint32Val
	case "uint64":
		dataRecordFieldTypeUint64Val, err := strconv.ParseUint(dataRecordFieldVal, 10, 64)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		recordMap[dataRecordFieldName] = dataRecordFieldTypeUint64Val
	}

	return nil
}
