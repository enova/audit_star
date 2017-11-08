package audit

import (
	"database/sql"
	"log"

	"github.com/lib/pq"
)

type column struct {
	primaryKey   sql.NullInt64
	oldID        sql.NullInt64
	newID        sql.NullInt64
	operation    sql.NullString
	beforeChange sql.NullString
	change       sql.NullString
	changedAt    pq.NullTime
	changedBy    sql.NullString
	oldColumn2   sql.NullString
	newColumn2   sql.NullString
	newColumn3   sql.NullFloat64
	id           sql.NullInt64
	column2      sql.NullString
	clientQuery  sql.NullString
	count        sql.NullInt64
	exists       sql.NullString
}

func getConfig(c *Config) {
	// Get Config from config file
	getconfigErr := GetConfig(c)
	if getconfigErr != nil {
		log.Fatal(getconfigErr)
	}
}

func setupDB(c *Config) *sql.DB {
	// Open DB
	db, dbErr := DBOpen(c)
	if dbErr != nil {
		log.Fatal(dbErr)
	}

	return db
}

// runallTest ...
func runAllTest() {
	var c Config
	// ParseFlags parses command-line flags
	// note: even though the tests are not using command-line flags, ParseFlags
	// does set some default values, such as the path to the config file
	ParseFlags(&c)
	getConfig(&c)

	db := setupDB(&c)
	defer db.Close()

	err := RunAll(db, &c)
	if err != nil {
		log.Fatal(err.Error())
	}
}
