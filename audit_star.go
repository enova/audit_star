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
	audit.ParseFlags(&c)

	// read config from file
	err := audit.GetConfig(&c)
	checkErr(err)

	// connect to the DB
	db, err := audit.DBOpen(&c)
	checkErr(err)

	// set up auditing on tables not excluded in the config
	err = audit.RunAll(db, &c)
	checkErr(err)
}
