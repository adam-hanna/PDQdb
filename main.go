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
	"encoding/json"
	"github.com/adam-hanna/PDQdb/data"
	error_ "github.com/adam-hanna/PDQdb/error"
	"github.com/adam-hanna/PDQdb/server"
	"github.com/codegangsta/cli"
	"log"
	"os"
)

func main() {
	app := cli.NewApp()
	app.Action = func(ctx *cli.Context) {
		csvConfigFilePath := ctx.GlobalString("config-file-path")
		if csvConfigFilePath == "" {
			log.Fatal(
				error_.New("--config-file-path (or -c) required!"),
			)
		}
		csvFilePath := ctx.GlobalString("file-path")
		if csvFilePath == "" {
			log.Fatal(
				error_.New("--file-path (or -f) required!"),
			)
		}
		// Open the JSON config file.
		csvConfigFileHandle, err := os.Open(csvConfigFilePath)
		if err != nil {
			log.Fatal(err)
		}
		defer csvConfigFileHandle.Close()
		// Open the CSV file.
		csvFileHandle, err := os.Open(csvFilePath)
		if err != nil {
			log.Fatal(err)
		}
		defer csvFileHandle.Close()
		// Get ready to start decoding the JSON config file.
		csvConfigFileJsonDecoder := json.NewDecoder(csvConfigFileHandle)
		var configJsonDescriptor data.ConfigJsonDescriptor
		err = csvConfigFileJsonDecoder.Decode(&configJsonDescriptor)
		if err != nil {
			log.Fatal(err)
		}
		// fmt.Println(configJsonDescriptor)
		data.LoadAndTransformCsvData(csvFileHandle, &configJsonDescriptor)
		err = server.StartServer(
			ctx.GlobalString("server-hostname"),
			uint16(ctx.GlobalInt("server-port")),
		)
		if err != nil {
			log.Fatal(err)
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
			Name:  "config-file-path,c",
			Usage: "Path to the JSON config file for the data set.",
		},
		cli.StringFlag{
			Name:  "file-path,f",
			Usage: "Path to the CSV file to load.",
		},
		cli.StringFlag{
			Name:  "server-hostname,n",
			Usage: "Server hostname.",
			Value: "localhost",
		},
		cli.IntFlag{
			Name:  "server-port,p",
			Usage: "Server port.",
			Value: 38216,
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
