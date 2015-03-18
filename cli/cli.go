package cli

import (
	"github.com/adam-hanna/PDQdb/data"
	error_ "github.com/adam-hanna/PDQdb/error"
	"github.com/codegangsta/cli"
	"log"
	"os"
)

func StartCLI(cliFlags *data.CliFlagsStruct) {
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
		serverHostname := ctx.GlobalString("server-hostname")
		serverPort := uint16(ctx.GlobalInt("server-port"))

		// build the cli struct to send back to main
		cliFlags.ConfigFilePath = csvConfigFilePath
		cliFlags.FilePath = csvFilePath
		cliFlags.ServerHostname = serverHostname
		cliFlags.ServerPort = serverPort
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
