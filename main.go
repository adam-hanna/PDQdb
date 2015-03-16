/*
 * Copyright (C) 2015 Adam Hanna <ahanna@alumni.mines.edu>
 * Copyright (C) 2015 Jonathan Barronville <jonathan@belairlabs.com>
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

package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// define a struct for our data
// can we define no 64 bit shiz? doing this will ensure we use the least amount of memory necessary
// we're assuming the first col is the ID and won't be stored (bc it will be the key)
type DataStruct struct {
	foo2 uint8
	foo3 int8
	foo4 float32
}

func main() {
	sFilePath, _ := filepath.Abs("20150315_randomData.txt")
	m := make(map[string]DataStruct)

	ToMem(sFilePath, m)

	fmt.Println(m["QG50"])
}

// Open a file and scan line by line
func ToMem(sFilePath string, m map[string]DataStruct) {
	file, err := os.Open(sFilePath)

	if err != nil {
		fmt.Println(err)
	}

	defer file.Close()

	// Note that scanner is limited to 4096 []byte buffer size per line!
	// User bufio.ReaderLine() or ReadString() instead bc no line limit?
	scanner := bufio.NewScanner(file)

	// get some vars ready to scan the file
	bHeaderRow := true

	// start reading the file line-by-line
	for scanner.Scan() {
		// skip the first row; there must be a better way!
		if bHeaderRow {
			bHeaderRow = false
		} else {
			// write this line to the map
			// assume the first col is the id; also assume this is a string
			// assume the delimiter is a tab
			// assume the number of columns is 4
			slTemp := strings.FieldsFunc(scanner.Text(), tabSlicer)

			//handle errors later
			tempFoo2, _ := strconv.ParseUint(slTemp[1], 10, 8)
			tempFoo3, _ := strconv.ParseInt(slTemp[2], 10, 8)
			tempFoo4, _ := strconv.ParseFloat(slTemp[3], 32)

			tempData := DataStruct{
				uint8(tempFoo2),
				int8(tempFoo3),
				float32(tempFoo4),
			}
			m[slTemp[0]] = tempData
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println(err)
	}
}

// this function slices a string by a rune
// the tab rune is hard coded, for now
func tabSlicer(r rune) bool {
	return r == '\t'
}
