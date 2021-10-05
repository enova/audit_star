package main

import (
	"log"

	"github.com/enova/audit_star/audit"
)

// error checker helper function
func checkErr(err error) {
	if err != nil {
		log.Fatalf("ERROR: %+v\n", err)
	}
}

func main() {
	var c audit.Config

	// parse command-line flags
	err := audit.ParseFlags(&c)
	checkErr(err)

	// read config from file
	err = audit.GetConfig(&c)
	checkErr(err)

	// override config file values with CLI flag values if specified
	err = audit.ParseCLIOverrides(&c)
	checkErr(err)

	// connect to the DB
	db, err := audit.DBOpen(&c)
	checkErr(err)

	// set up auditing on tables not excluded in the config
	err = audit.RunAll(db, &c)
	checkErr(err)
}
