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

package main

import (
	"encoding/csv"
	"fmt"
	error_ "github.com/adam-hanna/PDQdb/error"
	"github.com/codegangsta/cli"
	"io"
	"log"
	"os"
)

func main() {
	app := cli.NewApp()
	app.Action = func(ctx *cli.Context) {
		csvFilePath := ctx.GlobalString("file-path")
		if csvFilePath == "" {
			log.Fatal(
				error_.New("--file-path (or -f) required!"),
			)
		}
		csvFileHandle, err := os.Open(csvFilePath)
		if err != nil {
			log.Fatal(err)
		}
		defer csvFileHandle.Close()
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
			fmt.Printf("Record %d is %v and has %d fields.\n", csvFileLineCount, dataRecord, len(dataRecord))
			csvFileLineCount += 1
		}
	}
	app.Authors = []cli.Author{
		{
			Email: "ahanna@alumni.mines.edu",
			Name:  "Adam Hanna",
		},
		{
			Email: "jonathan@belairlabs.com",
			Name:  "Jonathan Barronville",
		},
	}
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "file-path,f",
			Usage: "Path to the CSV file to load.",
		},
	}
	app.Name = "PDQdb"
	app.Usage = "A read-optimized, in-memory, data processing engine."
	app.Version = "0.0.1"
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
