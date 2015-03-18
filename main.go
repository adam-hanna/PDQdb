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
	"github.com/adam-hanna/PDQdb/cli"
	"github.com/adam-hanna/PDQdb/data"
	"github.com/adam-hanna/PDQdb/server"
	"log"
)

func main() {
	// Grab the user inputed CLI flags
	cliFlags := data.CliFlagsStruct{}
	cli.StartCLI(&cliFlags)

	// Load the csv data into memory
	data.LoadAndTransformCsvData(cliFlags)

	// start the server
	err := server.StartServer(cliFlags.ServerHostname, cliFlags.ServerPort)
	if err != nil {
		log.Fatal(err)
	}
}
