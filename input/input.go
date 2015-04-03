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

package input

import (
	"encoding/csv"
	"encoding/json"
	// "fmt"
	"github.com/adam-hanna/PDQdb/cli"
	"github.com/adam-hanna/PDQdb/data"
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

var configJsonDescriptor configJsonDescriptorStruct

func LoadConfigFile(cliFlags cli.CliFlagsStruct) ([]interface{}, string) {
	// Open the JSON config file.
	csvConfigFileHandle, err := os.Open(cliFlags.ConfigFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer csvConfigFileHandle.Close()

	// Get ready to start decoding the JSON config file.
	csvConfigFileJsonDecoder := json.NewDecoder(csvConfigFileHandle)

	err = csvConfigFileJsonDecoder.Decode(&configJsonDescriptor)
	if err != nil {
		log.Fatal(err)
	}

	return configJsonDescriptor.Header, configJsonDescriptor.IdField
}

func LoadAndTransformCsvData(cliFlags cli.CliFlagsStruct) {
	// Open the CSV file.
	csvFileHandle, err := os.Open(cliFlags.FilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer csvFileHandle.Close()

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
		recordArray := make([]interface{}, 0)
		for idx, dataRecordFieldVal := range dataRecord {
			var dataRecordFieldTypeString string

			// There should always be only one iteration of this loop.
			for _, val := range configJsonDescriptor.Header[idx].(map[string]interface{}) {
				dataRecordFieldTypeString = val.(string)
			}
			if dataRecordFieldVal != "" {
				// convert the input string based on data type
				err = convertStringToType(&recordArray, dataRecordFieldVal, dataRecordFieldTypeString)
				if err != nil {
					log.Print(err)
				}
			} else {
				recordArray = append(recordArray, nil)
			}
		}

		// add the record to the array
		// Assumes the data set's key is always a string.
		data.SetData(recordArray)

		// add the necessary indexes
		if len(configJsonDescriptor.IndexFields) > 0 {
			index.AppendIndex(configJsonDescriptor.IndexFields, configJsonDescriptor.IdField, recordArray)
		}

		// next row of the csv
		csvFileLineCount += 1
	}
}

func convertStringToType(recordArray *[]interface{}, dataRecordFieldVal string, dataRecordFieldTypeString string) error {
	switch dataRecordFieldTypeString {
	case "string":
		*recordArray = append(*recordArray, dataRecordFieldVal)
	case "bool":
		dataRecordFieldTypeBoolVal, err := strconv.ParseBool(dataRecordFieldVal)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		*recordArray = append(*recordArray, dataRecordFieldTypeBoolVal)
	case "float32":
		dataRecordFieldTypeFloat32ValTmp, err := strconv.ParseFloat(dataRecordFieldVal, 32)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		dataRecordFieldTypeFloat32Val := float32(dataRecordFieldTypeFloat32ValTmp)
		*recordArray = append(*recordArray, dataRecordFieldTypeFloat32Val)
	case "float64":
		dataRecordFieldTypeFloat64Val, err := strconv.ParseFloat(dataRecordFieldVal, 64)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		*recordArray = append(*recordArray, dataRecordFieldTypeFloat64Val)
	case "int":
		dataRecordFieldTypeIntValTmp, err := strconv.ParseInt(dataRecordFieldVal, 10, 0)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		dataRecordFieldTypeIntVal := int(dataRecordFieldTypeIntValTmp)
		*recordArray = append(*recordArray, dataRecordFieldTypeIntVal)
	case "int8":
		dataRecordFieldTypeInt8ValTmp, err := strconv.ParseInt(dataRecordFieldVal, 10, 8)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		dataRecordFieldTypeInt8Val := int8(dataRecordFieldTypeInt8ValTmp)
		*recordArray = append(*recordArray, dataRecordFieldTypeInt8Val)
	case "int16":
		dataRecordFieldTypeInt16ValTmp, err := strconv.ParseInt(dataRecordFieldVal, 10, 16)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		dataRecordFieldTypeInt16Val := int16(dataRecordFieldTypeInt16ValTmp)
		*recordArray = append(*recordArray, dataRecordFieldTypeInt16Val)
	case "int32":
		dataRecordFieldTypeInt32ValTmp, err := strconv.ParseInt(dataRecordFieldVal, 10, 32)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		dataRecordFieldTypeInt32Val := int32(dataRecordFieldTypeInt32ValTmp)
		*recordArray = append(*recordArray, dataRecordFieldTypeInt32Val)
	case "int64":
		dataRecordFieldTypeInt64Val, err := strconv.ParseInt(dataRecordFieldVal, 10, 64)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			log.Print(err)
		}
		*recordArray = append(*recordArray, dataRecordFieldTypeInt64Val)
	case "uint":
		dataRecordFieldTypeUintValTmp, err := strconv.ParseUint(dataRecordFieldVal, 10, 0)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		dataRecordFieldTypeUintVal := uint(dataRecordFieldTypeUintValTmp)
		*recordArray = append(*recordArray, dataRecordFieldTypeUintVal)
	case "uint8":
		dataRecordFieldTypeUint8ValTmp, err := strconv.ParseUint(dataRecordFieldVal, 10, 8)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		dataRecordFieldTypeUint8Val := uint8(dataRecordFieldTypeUint8ValTmp)
		*recordArray = append(*recordArray, dataRecordFieldTypeUint8Val)
	case "uint16":
		dataRecordFieldTypeUint16ValTmp, err := strconv.ParseUint(dataRecordFieldVal, 10, 16)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		dataRecordFieldTypeUint16Val := uint16(dataRecordFieldTypeUint16ValTmp)
		*recordArray = append(*recordArray, dataRecordFieldTypeUint16Val)
	case "uint32":
		dataRecordFieldTypeUint32ValTmp, err := strconv.ParseUint(dataRecordFieldVal, 10, 32)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		dataRecordFieldTypeUint32Val := uint32(dataRecordFieldTypeUint32ValTmp)
		*recordArray = append(*recordArray, dataRecordFieldTypeUint32Val)
	case "uint64":
		dataRecordFieldTypeUint64Val, err := strconv.ParseUint(dataRecordFieldVal, 10, 64)
		if err != nil {
			// NOTE(@jonathanmarvens): Should be Fatal?
			return err
		}
		*recordArray = append(*recordArray, dataRecordFieldTypeUint64Val)
	}

	return nil
}
