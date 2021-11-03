package audit

import (

	// postgres driver
	"bytes"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"text/template"

	yaml "gopkg.in/yaml.v2"
)

// Config ...
type Config struct {
	CfgPath         string
	Host            string   `yaml:"host"`
	Port            string   `yaml:"port"`
	DBName          string   `yaml:"db_name"`
	DBUser          string   `yaml:"username"`
	DBPassword      string   `yaml:"password"`
	SSLMode         string   `yaml:"ssl_mode"`
	ExcludedTables  []string `yaml:"excluded_tables"`
	ExcludedSchemas []string `yaml:"excluded_schemas"`
	IncludedTables  []string `yaml:"included_tables"`
	Security        string   `yaml:"security"`
	LogClientQuery  bool     `yaml:"log_client_query"`
	Owner           string   `yaml:"owner"`
	ViewsOnly       bool     `yaml:"views_only"`
	Grantee         string   `yaml:"grantee"`
	OwnerRole       string   `yaml:"set_role"`
	LockTimeout     string   `yaml:"lock_timeout"`
	JSONType        string
}

type tableSettings struct {
	enableTable   bool
	enableTrigger bool
}

var cfgPath = flag.String("cfg", "audit.yml", "Path to config file used by audit_star.")
var selectedTable = flag.String("table", "", "A single fully-qualified table name to be provisioned for auditing.")

var errorCounter int

// ParseFlags parses command line flags for configration from command line input
func ParseFlags(c *Config) error {
	flag.Parse()
	c.CfgPath = *cfgPath

	return nil
}

func ParseTableName(tableName string) ([]string, error) {
	tableParts := strings.Split(tableName, ".")
	if len(tableParts) > 1 {
		return tableParts, nil
	}

	return nil, fmt.Errorf("table should be specified in the following format: schemaname.tablename")
}

// GetConfig pulls config info from audit.yml and command line input
func GetConfig(c *Config) error {
	file, err := ioutil.ReadFile(c.CfgPath)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(file, &c)
	if err != nil {
		return err
	}

	return nil
}

// ParseCLI Overrides parses CLI override flags
func ParseCLIOverrides(c *Config) error {
	// CLI overrides
	if *selectedTable == "" {
		return nil
	}

	c.IncludedTables = []string{*selectedTable}
	return nil
}

// DBOpen opens the db connection
func DBOpen(c *Config) (*sql.DB, error) {
	dbInfo := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.DBUser, c.DBName, c.SSLMode)
	if c.DBPassword != "" {
		dbInfo += fmt.Sprintf(" password=%s", c.DBPassword)
	}

	db, err := sql.Open("postgres", dbInfo)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	log.Println("successfully connected to", c.DBName)

	err = setOwnerRole(db, c)
	if err != nil {
		return nil, err
	}

	setLockTimeout(db, c)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// RunAll makes a list of all of the db's tables, marks which tables to exclude
// based on the config, then loops over all the tables and sets up auditting
// for each table
func RunAll(db *sql.DB, config *Config) error {
	// query the db for a list of all of its schemas
	allSchemas, err := getAllSchemas(db, config)
	if err != nil {
		return err
	}

	filteredScehmas := filterSchemas(allSchemas, config)

	// use the results from above to get a list of all of the tables in the db
	allTables, err := getAllTables(db, config, allSchemas)
	if err != nil {
		return err
	}

	// exclude tables from audit based on ExcludedSchemas in config
	filteredTables := filterTableSchemas(allTables, config)

	// exclude tables from audit based on ExcludedTables in config
	filteredTables = filterTables(filteredTables, config)

	// having this set in the db is a pre-condition of running audit_star
	err = ensureSettingExists("audit_star.changed_by", db)
	if err != nil {
		return err
	}

	err = createAuditSchema(db)
	if err != nil {
		return err
	}

	err = createAuditAuditingTable(db)
	if err != nil {
		return err
	}

	err = createNoDMLAuditFunction(db)
	if err != nil {
		return err
	}

	config.JSONType, err = getSupportedJSONType(db)
	if err != nil {
		return err
	}

	err = createRawAuditSchemas(db, config, filteredScehmas)
	if err != nil {
		return err
	}

	err = grantUsageOnSchemas(db, config, filteredScehmas)
	if err != nil {
		return err
	}

	log.Println("finished granting usage on schemas")

	// calls all of the code which sets up all of the auditing dbs and triggers
	err = setAuditing(filteredTables, config, db)
	if err != nil {
		return err
	}

	if errorCounter == 0 {
		log.Println("auditing setup completed without errors")
	} else {
		log.Println(fmt.Sprintf("auditing setup completed with %d errors", errorCounter))
	}

	return nil
}

func setOwnerRole(db *sql.DB, c *Config) error {
	if c.OwnerRole != "" {
		_, err := db.Exec(fmt.Sprintf(`set role='%s'`, c.OwnerRole))
		return err
	}
	return nil
}

func setLockTimeout(db *sql.DB, c *Config) error {
	if c.LockTimeout != "" {
		_, err := db.Exec(fmt.Sprintf(`set lock_timeout='%s'`, c.LockTimeout))
		return err
	}
	return nil
}

// returns a slice of schema names in the db
func getAllSchemas(db *sql.DB, c *Config) ([]string, error) {
	query := `SELECT schema_name AS schema
	FROM information_schema.schemata
	WHERE schema_name NOT LIKE '%audit%'
	AND schema_name NOT LIKE 'pg\_%'
	AND schema_name NOT IN ('public', 'information_schema')`

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schema string
	var schemas []string
	for rows.Next() {
		err := rows.Scan(&schema)
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, schema)
	}

	return schemas, nil
}

// returns a map of table names in the schema to be used later for
// determining which tables have their audit triggers enabled
func getAllTables(db *sql.DB, c *Config, schemas []string) (map[string]tableSettings, error) {
	allTables := make(map[string]tableSettings)
	for _, schema := range schemas {
		tables, err := tablesForSchema(db, c, schema)
		if err != nil {
			return nil, err
		}

		for _, table := range tables {
			schemaTable := schema + "." + table
			allTables[schemaTable] = tableSettings{
				enableTable:   true,
				enableTrigger: true,
			}
		}
	}

	return allTables, nil
}

// returns a slice of table names for a given schema
func tablesForSchema(db *sql.DB, c *Config, schema string) ([]string, error) {
	query := `SELECT relname AS table
		FROM pg_class
		JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
		JOIN pg_roles ON pg_roles.oid = pg_class.relowner
		WHERE nspname = $1
		AND relkind = 'r'
		AND NOT relisshared`

	if c.Owner != "" {
		query += " AND rolname = '" + c.Owner + "'"
	}

	printQueryIfDebug(query)
	rows, err := db.Query(query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var table string
	var tables []string
	for rows.Next() {
		err := rows.Scan(&table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}

	return tables, nil
}

func filterSchemas(schemas []string, c *Config) []string {
	var filteredSchemas []string
	for _, schema := range schemas {
		if isIncludedSchema(schema, c) {
			if !isExcludedSchema(schema, c) {
				filteredSchemas = append(filteredSchemas, schema)
			}
		}
	}
	return filteredSchemas
}

// turn off auditting on specific schemas based on config
func filterTableSchemas(tables map[string]tableSettings, c *Config) map[string]tableSettings {
	for table := range tables {
		if isExcludedSchema(table, c) {
			tables[table] = tableSettings{
				enableTable:   false,
				enableTrigger: false,
			}
		}
	}

	return tables
}

func isExcludedSchema(table string, c *Config) bool {
	for _, schema := range c.ExcludedSchemas {
		if strings.HasPrefix(table, schema) {
			return true
		}
	}

	return false
}

// turn off auditting on specific tables based on config
func filterTables(tables map[string]tableSettings, c *Config) map[string]tableSettings {
	for table := range tables {
		if !isIncludedTable(table, c) {
			tables[table] = tableSettings{
				enableTable:   false,
				enableTrigger: false,
			}
		}

		if isExcludedTable(table, c) {
			tables[table] = tableSettings{
				enableTable:   false,
				enableTrigger: false,
			}
		}
	}

	return tables
}

func isIncludedSchema(table string, c *Config) bool {
	if len(c.IncludedTables) == 0 {
		return true
	}

	for _, t := range c.IncludedTables {
		if idx := strings.IndexByte(t, '.'); idx >= 0 {
			if table == t[:idx] {
				return true
			}
		}
	}

	return false
}

func isIncludedTable(table string, c *Config) bool {
	if len(c.IncludedTables) == 0 {
		return true
	}

	for _, t := range c.IncludedTables {
		if table == t {
			return true
		}
	}

	return false
}

func isExcludedTable(table string, c *Config) bool {
	for _, t := range c.ExcludedTables {
		if table == t {
			return true
		}
	}

	return false
}

// loops over each table in the db and sets up auditting for that table
func setAuditing(tables map[string]tableSettings, c *Config, db *sql.DB) error {
	for tbl, tableSettings := range tables {
		if tableSettings.enableTable {
			schemaTable := strings.Split(tbl, ".")
			schema := schemaTable[0]
			table := schemaTable[1]
			validPrimaryKey, err := hasValidPrimaryKey(schema, table, db)
			if err != nil {
				return err
			}

			if validPrimaryKey {
				if c.ViewsOnly {
					err := auditViewsOnly(schema, table, tableSettings.enableTrigger, c, db)
					if err != nil {
						return err
					}
				} else {
					err := audit(schema, table, tableSettings.enableTrigger, c, db)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

// sets up audting for a given table, as configured in the config file
// func audit(schema, table, security string, logging, trigger bool, db *sql.DB) error {
func audit(schema, table string, trigger bool, c *Config, db *sql.DB) error {
	err := addColToTable(schema, table, "updated_by", "varchar(50)", db)
	if err != nil {
		return err
	}

	auditSchema := schema + "_audit_raw"
	err = createAuditTable(auditSchema, table, c.JSONType, db)
	if err != nil {
		return err
	}

	err = addColToTable(auditSchema, table+"_audit", "sparse_time", "timestamptz", db)
	if err != nil {
		return err
	}

	err = addColToTable(auditSchema, table+"_audit", "before_change", c.JSONType, db)
	if err != nil {
		return err
	}

	err = addColToTable(auditSchema, table+"_audit", "changed_by", "varchar(50)", db)
	if err != nil {
		return err
	}

	tablesToGrant := []string{
		"\"" + auditSchema + "\".\"" + table + "_audit\"",
	}
	err = grantSelectOnTable(db, c, tablesToGrant)
	if err != nil {
		return err
	}

	err = createAuditIndex(auditSchema, table, db)
	if err != nil {
		return err
	}

	err = createAuditFunction(schema, table, c.JSONType, c.Security, c.LogClientQuery, db)
	if err != nil {
		return err
	}

	err = createAuditTrigger(schema, table, trigger, db)
	if err != nil {
		return err
	}

	err = createViewAuditSchema(schema, db)
	if err != nil {
		return err
	}

	err = grantUsageOnSchemas(db, c, []string{schema})
	if err != nil {
		return err
	}

	tableCols, err := tableColumns(schema, table, db)
	if err != nil {
		return err
	}

	primaryKeyCol := getPrimaryKeyCol(tableCols)

	err = createAuditDeltaView(schema, table, c.Grantee, tableCols, primaryKeyCol, db)
	if err != nil {
		return err
	}

	err = createAuditSnapshotView(schema, table, c.Grantee, tableCols, primaryKeyCol, db)
	if err != nil {
		return err
	}

	err = createAuditCompareView(schema, table, c.Grantee, tableCols, primaryKeyCol, db)
	if err != nil {
		return err
	}

	return nil
}

// sets up audting for a given table, as configured in the config file
func auditViewsOnly(schema, table string, trigger bool, c *Config, db *sql.DB) error {
	err := addColToTable(schema, table, "updated_by", "varchar(50)", db)
	if err != nil {
		return err
	}

	tableCols, err := tableColumns(schema, table, db)
	if err != nil {
		return err
	}

	primaryKeyCol := getPrimaryKeyCol(tableCols)

	err = createAuditDeltaView(schema, table, c.Grantee, tableCols, primaryKeyCol, db)
	if err != nil {
		return err
	}

	err = createAuditSnapshotView(schema, table, c.Grantee, tableCols, primaryKeyCol, db)
	if err != nil {
		return err
	}

	err = createAuditCompareView(schema, table, c.Grantee, tableCols, primaryKeyCol, db)
	if err != nil {
		return err
	}

	return nil
}

// helper method to DRY up the code that parses a query template using data
func mustParseQuery(query string, data map[string]interface{}) string {
	printQueryIfDebug(query)
	t := template.Must(template.New("template").Parse(query))
	buf := &bytes.Buffer{}
	if err := t.Execute(buf, data); err != nil {
		log.Fatal(err)
	}

	return buf.String()
}

// used to check that a setting exists in the db before proceeding
// specifically used to check that audit_star.changed_by field is set
func ensureSettingExists(setting string, db *sql.DB) error {
	query := `DO
		$$
		BEGIN
			BEGIN
				PERFORM current_setting('%s');
			EXCEPTION WHEN undefined_object THEN
				RAISE EXCEPTION 'SQLERRM: %, please contact your friendly, neighbourhood DBA.', SQLERRM;
			END;
		END;
		$$
		LANGUAGE plpgsql;`
	printQueryIfDebug(fmt.Sprintf(query, setting))
	_, err := db.Exec(fmt.Sprintf(query, setting))
	if err != nil {
		return err
	}

	log.Println(setting, "found")
	return nil
}

// creates the audit schema
func createAuditSchema(db *sql.DB) error {
	query := `DO
		$$
		BEGIN
			IF NOT EXISTS (
					SELECT 1
					FROM information_schema.schemata
					WHERE schema_name = 'audit'
			) THEN
				CREATE SCHEMA audit;
			END IF;
		END;
		$$
		LANGUAGE plpgsql;`
	printQueryIfDebug(query)
	_, err := db.Exec(query)
	if err != nil {
		return err
	}
	log.Println("audit schema created")
	return nil
}

// creates the audit.audit_history table
func createAuditAuditingTable(db *sql.DB) error {
	query := `CREATE TABLE IF NOT EXISTS audit.audit_history(
		audit_history_id SERIAL PRIMARY KEY,
		schema_name NAME NOT NULL,
		table_name NAME NOT NULL,
		start_time TIMESTAMPTZ NOT NULL,
		end_time TIMESTAMPTZ,
		CONSTRAINT uniq UNIQUE(schema_name, table_name, start_time)
	)`
	printQueryIfDebug(query)
	_, err := db.Exec(query)
	if err != nil {
		return err
	}

	log.Println("audit auditing table created")
	return nil
}

func createNoDMLAuditFunction(db *sql.DB) error {
	query := `CREATE OR REPLACE FUNCTION audit.no_dml_on_audit_table()
		RETURNS TRIGGER AS
		$$
		BEGIN
			RAISE EXCEPTION 'No common-case updates/deletes/truncates allowed on audit table';
			RETURN NULL;
		END;
		$$
		LANGUAGE plpgsql;`
	printQueryIfDebug(query)
	_, err := db.Exec(query)
	if err != nil {
		return err
	}

	log.Println("no-DML audit function created")
	return nil
}

// adds a column of a given type to a db's schema.table
func addColToTable(schema, table, column, colType string, db *sql.DB) error {
	data := map[string]interface{}{
		"schema":  schema,
		"table":   table,
		"column":  column,
		"colType": colType,
	}

	query := `DO
		$$
		BEGIN
			BEGIN
				ALTER TABLE "{{.schema}}"."{{.table}}" ADD COLUMN "{{.column}}" {{.colType}};
			EXCEPTION
				WHEN duplicate_column THEN RAISE NOTICE 'column <"{{.column}}"> already exists in <"{{.schema}}"."{{.table}}">';
			END;
		END;
		$$`

	_, err := db.Exec(mustParseQuery(query, data))
	if err != nil {
		return err
	}

	log.Printf("added %s column to %s.%s\n", column, schema, table)
	return nil
}

// helper function used below to make sure we don't create audit schemas
// for excluded schemas
func contains(a []string, s string) bool {
	fmt.Println("inside contains function")
	fmt.Println("a:", a)
	fmt.Println("s:", s)
	for index, item := range a {
		fmt.Println("index:", index)
		fmt.Println("item:", item)
		if item == s {
			return true
		}
	}

	return false
}

// creates _audit_raw schemas for all non-excluded schemas
func createRawAuditSchemas(db *sql.DB, c *Config, schemas []string) error {
	for _, schema := range schemas {
		query := `DO
			$$
			BEGIN
				IF NOT EXISTS (
						SELECT 1
						FROM information_schema.schemata
						WHERE schema_name = '%s_audit_raw'
				) THEN
					CREATE SCHEMA "%s_audit_raw";
				END IF;
			END;
			$$
			LANGUAGE plpgsql;`
		printQueryIfDebug(fmt.Sprintf(query, schema, schema))
		_, err := db.Exec(fmt.Sprintf(query, schema, schema))
		if err != nil {
			return err
		}
		log.Printf("%s_audit_raw created\n", schema)
	}

	return nil
}

func grantUsageOnSchemas(db *sql.DB, c *Config, schemas []string) error {
	for _, schema := range schemas {
		query := `GRANT USAGE ON SCHEMA "%s_audit_raw" TO %s;`
		_, err := db.Exec(fmt.Sprintf(query, schema, c.Grantee))
		if err != nil {
			return err
		}
		log.Printf("granted usage on schema %s_audit_raw to %s\n", schema, c.Grantee)
	}

	return nil
}

func grantSelectOnTable(db *sql.DB, c *Config, tables []string) error {
	for _, table := range tables {
		query := `GRANT SELECT ON TABLE %s TO %s;`
		printQueryIfDebug(fmt.Sprintf(query, table, c.Grantee))
		_, err := db.Exec(fmt.Sprintf(query, table, c.Grantee))
		if err != nil {
			return err
		}
		log.Printf("granted select on table to %s\n", c.Grantee)
	}

	return nil
}

// queries the db to determine which JSON type is supported by the host db
func getSupportedJSONType(db *sql.DB) (string, error) {
	query := `SELECT EXISTS (
		SELECT 1
		FROM pg_type
		WHERE typname LIKE 'jsonb'
	) AS exists`
	printQueryIfDebug(query)
	row := db.QueryRow(query)

	var jsonBExists bool
	err := row.Scan(&jsonBExists)
	if err != nil {
		return "", err
	}

	if jsonBExists {
		log.Println("db supports jsonb")
		return "jsonb", nil
	}

	log.Println("db does not support jsonb, will use json instead")
	return "json", nil
}

// creates the audit table for a given table
func createAuditTable(auditSchema, table, jsonType string, db *sql.DB) error {
	data := map[string]interface{}{
		"auditSchema": auditSchema,
		"table":       table,
		"jsonType":    jsonType,
	}

	query := `CREATE TABLE IF NOT EXISTS "{{.auditSchema}}"."{{.table}}_audit"(
			"{{.table}}_audit_id" BIGSERIAL PRIMARY KEY,
			changed_at TIMESTAMPTZ NOT NULL,
			db_user VARCHAR(50) NOT NULL,
			client_addr INET,
			client_port INTEGER,
			client_query TEXT,
			operation VARCHAR(1) NOT NULL,
			before_change {{.jsonType}},
			change {{.jsonType}},
			primary_key TEXT
		);

		ALTER TABLE "{{.auditSchema}}"."{{.table}}_audit" ALTER COLUMN client_query DROP NOT NULL;

		BEGIN;
			DROP TRIGGER IF EXISTS no_dml_on_audit_table ON "{{.auditSchema}}"."{{.table}}_audit";
			CREATE TRIGGER no_dml_on_audit_table
			BEFORE UPDATE OR DELETE ON "{{.auditSchema}}"."{{.table}}_audit"
			FOR EACH ROW
			EXECUTE PROCEDURE audit.no_dml_on_audit_table();

			DROP TRIGGER IF EXISTS no_truncate_on_audit ON "{{.auditSchema}}"."{{.table}}_audit";
			CREATE TRIGGER no_truncate_on_audit
			BEFORE TRUNCATE ON "{{.auditSchema}}"."{{.table}}_audit"
			FOR EACH STATEMENT
			EXECUTE PROCEDURE audit.no_dml_on_audit_table();
		COMMIT;`

	_, err := db.Exec(mustParseQuery(query, data))
	if err != nil {
		return err
	}

	log.Printf("created audit schema %s.%s_audit\n", auditSchema, table)
	return nil
}

// created the index on an audit table
func createAuditIndex(auditSchema, table string, db *sql.DB) error {
	data := map[string]interface{}{
		"auditSchema": auditSchema,
		"table":       table,
	}

	query := `DO
		$$
		BEGIN
			IF NOT EXISTS (
					SELECT 1
					FROM pg_class c
					JOIN pg_namespace n ON n.oid = c.relnamespace
					WHERE c.relname = 'index_{{.table}}_on_primary_key'
					AND n.nspname = '{{.auditSchema}}'
			) THEN

				CREATE INDEX "index_{{.table}}_on_primary_key" ON "{{.auditSchema}}"."{{.table}}_audit"(primary_key);
				CREATE INDEX "index_{{.table}}_on_sparse_time" ON "{{.auditSchema}}"."{{.table}}_audit"(sparse_time) WHERE sparse_time IS NOT NULL;

			END IF;
		END;
		$$
		LANGUAGE plpgsql;`

	_, err := db.Exec(mustParseQuery(query, data))
	if err != nil {
		return err
	}

	log.Printf("created audit index on %s.%s_audit\n", auditSchema, table)
	return nil
}

// creates the audit function for a table
func createAuditFunction(schema, table, jsonType, security string, logging bool, db *sql.DB) error {
	query := `SELECT DISTINCT(objid::regclass) AS sequence_name
		FROM pg_depend
		JOIN pg_index ON indrelid = refobjid
		JOIN pg_attribute ON attrelid = refobjid AND attnum = refobjsubid AND attnum = ANY(indkey)
		JOIN pg_class ON objid = pg_class.oid AND pg_class.relkind = 'S'
		WHERE refobjid = '%s_audit_raw.%s_audit'::regclass
		AND refobjsubid > 0
		AND indisprimary`

	queryString := fmt.Sprintf(query, schema, table)
	var sequenceName string
	printQueryIfDebug(queryString)
	err := db.QueryRow(queryString).Scan(&sequenceName)
	if err != nil {
		return err
	}

	query = `CREATE OR REPLACE FUNCTION "{{.schema}}_audit_raw"."audit_{{.schema}}_{{.table}}"()
		RETURNS TRIGGER AS
		$$
		DECLARE
			value_row HSTORE = hstore(NULL);
			new_row HSTORE = hstore(NULL);
			sparse_time TIMESTAMPTZ = NULL;
			audit_id BIGINT;
		BEGIN
			SELECT nextval('{{.sequenceName}}') INTO audit_id;
			IF (audit_id % 1000 = 0) THEN
				sparse_time = now();
			ELSE
				sparse_time = NULL;
			END IF;
			IF (TG_OP = 'UPDATE') THEN
				new_row = hstore(NEW);
				SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h.h)).key AS key, substring((each(h.h)).value FROM 1 FOR 500) AS value FROM (SELECT hstore(OLD) - hstore(NEW) AS h) h) sq;
				IF new_row ? TG_ARGV[0] THEN
					INSERT INTO "{{.schema}}_audit_raw"."{{.table}}_audit"("{{.table}}_audit_id", changed_at, changed_by, sparse_time, db_user, client_addr, client_port, client_query, operation, before_change, change, primary_key)
					VALUES(audit_id, now(), current_setting('audit_star.changed_by'), sparse_time, session_user::TEXT, inet_client_addr(), inet_client_port(), {{.clientQuery}}, substring(TG_OP,1,1), hstore_to_{{.jsonType}}(value_row), hstore_to_{{.jsonType}}(hstore(NEW) - hstore(OLD)), new_row -> TG_ARGV[0]);
				ELSE
					INSERT INTO "{{.schema}}_audit_raw"."{{.table}}_audit"("{{.table}}_audit_id", changed_at, changed_by, sparse_time, db_user, client_addr, client_port, client_query, operation, before_change, change, primary_key)
					VALUES(audit_id, now(), current_setting('audit_star.changed_by'), sparse_time, session_user::TEXT, inet_client_addr(), inet_client_port(), {{.clientQuery}}, substring(TG_OP,1,1), hstore_to_{{.jsonType}}(value_row), hstore_to_{{.jsonType}}(hstore(NEW) - hstore(OLD)), NULL);
				END IF;
			ELSIF (TG_OP = 'INSERT') THEN
				value_row = hstore(NEW);
				IF value_row ? TG_ARGV[0] THEN
					INSERT INTO "{{.schema}}_audit_raw"."{{.table}}_audit"("{{.table}}_audit_id", changed_at, changed_by, sparse_time, db_user, client_addr, client_port, client_query, operation, before_change, change, primary_key)
					VALUES(audit_id, now(), current_setting('audit_star.changed_by'), sparse_time, session_user::TEXT, inet_client_addr(), inet_client_port(), {{.clientQuery}}, substring(TG_OP,1,1), NULL, NULL, value_row -> TG_ARGV[0]);
				ELSE
					INSERT INTO "{{.schema}}_audit_raw"."{{.table}}_audit"("{{.table}}_audit_id", changed_at, changed_by, sparse_time, db_user, client_addr, client_port, client_query, operation, before_change, change, primary_key)
					VALUES(audit_id, now(), current_setting('audit_star.changed_by'), sparse_time, session_user::TEXT, inet_client_addr(), inet_client_port(), {{.clientQuery}}, substring(TG_OP,1,1), NULL, NULL, NULL);
				END IF;
			ELSIF (TG_OP = 'DELETE') THEN
				SELECT hstore(array_agg(sq.key), array_agg(sq.value)) INTO value_row FROM (SELECT (each(h)).key AS key, substring((each(h)).value FROM 1 FOR 500) AS value FROM hstore(OLD) h) sq;
				IF value_row ? TG_ARGV[0] THEN
					INSERT INTO "{{.schema}}_audit_raw"."{{.table}}_audit"("{{.table}}_audit_id", changed_at, changed_by, sparse_time, db_user, client_addr, client_port, client_query, operation, before_change, change, primary_key)
					VALUES(audit_id, now(), current_setting('audit_star.changed_by'), sparse_time, session_user::TEXT, inet_client_addr(), inet_client_port(), {{.clientQuery}}, substring(TG_OP,1,1), hstore_to_{{.jsonType}}(value_row), NULL, value_row -> TG_ARGV[0]);
				ELSE
					INSERT INTO "{{.schema}}_audit_raw"."{{.table}}_audit"("{{.table}}_audit_id", changed_at, changed_by, sparse_time, db_user, client_addr, client_port, client_query, operation, before_change, change, primary_key)
					VALUES(audit_id, now(), current_setting('audit_star.changed_by'), sparse_time, session_user::TEXT, inet_client_addr(), inet_client_port(), {{.clientQuery}}, substring(TG_OP,1,1), hstore_to_{{.jsonType}}(value_row), NULL, NULL);
				END IF;
			ELSIF (TG_OP = 'TRUNCATE') THEN
				INSERT INTO "{{.schema}}_audit_raw"."{{.table}}_audit"("{{.table}}_audit_id", changed_at, changed_by, sparse_time, db_user, client_addr, client_port, client_query, operation, before_change, change, primary_key)
				VALUES(audit_id, now(), current_setting('audit_star.changed_by'), sparse_time, session_user::TEXT, inet_client_addr(), inet_client_port(), {{.clientQuery}}, substring(TG_OP,1,1), NULL, NULL, NULL);
			ELSE
				RETURN NULL;
			END IF;

			RETURN NULL;
		END;
		$$
		LANGUAGE plpgsql
		SECURITY {{.security}};`

	var clientQuery string
	if logging {
		clientQuery = "substring(current_query(), 1, 1000)"
	} else {
		clientQuery = "NULL"
	}

	data := map[string]interface{}{
		"schema":       schema,
		"table":        table,
		"sequenceName": sequenceName,
		"jsonType":     jsonType,
		"clientQuery":  clientQuery,
		"security":     security,
	}

	_, err = db.Exec(mustParseQuery(query, data))
	if err != nil {
		return err
	}

	log.Printf("created audit function for %s_audit_raw.audit_%s_%s\n", schema, schema, table)
	return nil
}

// creates the trigger which records the changes to the audit table
// all tables have triggers created but those excluded by the config are disabled
func createAuditTrigger(schema, table string, enabled bool, db *sql.DB) error {
	query := `SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		JOIN pg_class ON i.indrelid = pg_class.oid
		JOIN pg_namespace on pg_class.relnamespace = pg_namespace.oid
		WHERE i.indisprimary
		AND nspname = '%s'
		AND relname = '%s'`
	printQueryIfDebug(fmt.Sprintf(query, schema, table))
	rows, err := db.Query(fmt.Sprintf(query, schema, table))
	if err != nil {
		return err
	}
	defer rows.Close()

	var pk string
	var primaryKeys []string
	for rows.Next() {
		err = rows.Scan(&pk)
		if err != nil {
			return err
		}
		primaryKeys = append(primaryKeys, pk)
	}

	data := map[string]interface{}{
		"schema": schema,
		"table":  table,
	}

	query = `BEGIN;
		DROP TRIGGER IF EXISTS row_audit_star ON "{{.schema}}"."{{.table}}";
		DROP TRIGGER IF EXISTS statement_audit_star ON "{{.schema}}"."{{.table}}";`

	if len(primaryKeys) == 1 {
		query += `CREATE TRIGGER row_audit_star
			AFTER INSERT OR UPDATE OR DELETE ON "{{.schema}}"."{{.table}}"
			FOR EACH ROW
			EXECUTE PROCEDURE "{{.schema}}_audit_raw"."audit_{{.schema}}_{{.table}}"('{{.primaryKey}}');

			CREATE TRIGGER statement_audit_star
			AFTER TRUNCATE ON "{{.schema}}"."{{.table}}"
			FOR EACH STATEMENT
			EXECUTE PROCEDURE "{{.schema}}_audit_raw"."audit_{{.schema}}_{{.table}}"('{{.primaryKey}}');`

		data["primaryKey"] = primaryKeys[0]
	} else {
		query += `CREATE TRIGGER row_audit_star
			AFTER INSERT OR UPDATE OR DELETE ON "{{.schema}}"."{{.table}}"
			FOR EACH ROW
			EXECUTE PROCEDURE "{{.schema}}_audit_raw"."audit_{{.schema}}_{{.table}}"();

			CREATE TRIGGER statement_audit_star
			AFTER TRUNCATE ON "{{.schema}}"."{{.table}}"
			FOR EACH STATEMENT
			EXECUTE PROCEDURE "{{.schema}}_audit_raw"."audit_{{.schema}}_{{.table}}"();`
	}

	if enabled {
		query += `DO
			$$
			BEGIN
				IF NOT EXISTS (SELECT 1 FROM audit.audit_history WHERE schema_name = '{{.schema}}' AND table_name = '{{.table}}' AND end_time IS NULL) THEN
					INSERT INTO audit.audit_history(schema_name, table_name, start_time) VALUES('{{.schema}}', '{{.table}}', now());
				END IF;
			END
			$$;`
	} else {
		query += `ALTER TABLE "{{.schema}}"."{{.table}}" DISABLE TRIGGER row_audit_star;
			ALTER TABLE "{{.schema}}"."{{.table}}" DISABLE TRIGGER statement_audit_star;`

		// must break since ddl replication w/ pg_logical using our in-house
		// extension cannot handle mixed DDL/DML in the same client statement

		_, err = db.Exec(mustParseQuery(query, data))
		if err != nil {
			return err
		}

		query = `UPDATE audit.audit_history SET end_time = now()
			WHERE schema_name = '{{.schema}}'
			AND table_name = '{{.table}}' AND end_time IS NULL;`
	}
	query += "COMMIT;"

	_, err = db.Exec(mustParseQuery(query, data))
	if err != nil {
		return err
	}

	log.Printf("audit trigger created for %s.%s enabled:%v\n", schema, table, enabled)
	return nil
}

// creates a view to aid in querying the db for what has changed
func createAuditDeltaView(schema, table, grantee string, tableCols []map[string]string, primaryKeyCol map[string]string, db *sql.DB) error {
	query := `
		DROP VIEW IF EXISTS "{{.schema}}_audit"."{{.table}}_audit_delta";
		CREATE VIEW "{{.schema}}_audit"."{{.table}}_audit_delta" AS
		SELECT "{{.table}}_audit_id",
						"{{.table}}_audit".primary_key AS primary_key,
						"{{.table}}_audit".changed_at AS audited_changed_at,
						"{{.table}}_audit".operation AS audited_operation,
						"{{.table}}_audit".db_user AS audited_db_user,
						"{{.table}}_audit".changed_by AS audited_change_agent,`

	data := map[string]interface{}{
		"schema":  schema,
		"table":   table,
		"grantee": grantee,
	}

	query = mustParseQuery(query, data)

	for _, col := range tableCols {
		q := `(before_change ->> '{{.colName}}')::{{.dataType}} AS "old_{{.colName}}",
			CASE WHEN "{{.table}}_audit".operation = 'I' THEN COALESCE(
				(
					SELECT DISTINCT ON (primary_key) (before_change ->> '{{.colName}}')::{{.dataType}}
					FROM "{{.schema}}_audit_raw"."{{.table}}_audit" spa
					WHERE spa.primary_key = "{{.table}}_audit".primary_key
					AND spa."{{.table}}_audit_id" > "{{.table}}_audit"."{{.table}}_audit_id"
					AND (before_change -> '{{.colName}}') IS NOT NULL
					ORDER BY primary_key, spa."{{.table}}_audit_id"
				),`

		if primaryKeyCol != nil {
			q += `("{{.table}}_json" ->> '{{.colName}}')::{{.dataType}}`
		} else {
			q += "NULL"
		}

		q += `)
			ELSE (change ->> '{{.colName}}')::{{.dataType}}
			END AS "new_{{.colName}}",`

		data = map[string]interface{}{
			"colName":    col["colName"],
			"dataType":   col["dataType"],
			"primaryKey": col["primaryKey"],
			"schema":     schema,
			"table":      table,
		}

		query += mustParseQuery(q, data)
	}

	query = strings.TrimSuffix(query, ",")

	q := ` FROM "{{.schema}}_audit_raw"."{{.table}}_audit" `

	if primaryKeyCol != nil {
		q += `LEFT JOIN "{{.schema}}"."{{.table}}"
			ON "{{.table}}_audit".primary_key::{{.pkcDataType}} = "{{.schema}}"."{{.table}}"."{{.pkcColName}}"
			LEFT JOIN LATERAL row_to_json("{{.table}}".*) "{{.table}}_json" ON TRUE `
		data["pkcDataType"] = primaryKeyCol["dataType"]
		data["pkcColName"] = primaryKeyCol["colName"]
	}

	if grantee != "" {
		q += "; GRANT SELECT ON \"{{.schema}}_audit\".\"{{.table}}_audit_delta\" TO " + grantee + "; "
	} else {
		q += "; "
	}

	query += mustParseQuery(q, data)

	tx, txErr := db.Begin()
	if txErr != nil {
		return txErr
	}

	_, err := tx.Exec(query)
	if err != nil {
		tx.Rollback()
		log.Println("error occurred while creating delta view: ", err)
		errorCounter++
		return nil
	}

	if err = tx.Commit(); err != nil {
		log.Println("error committing delta view transaction: ", err)
		return nil
	}

	log.Printf("created view %s_audit.%s_audit_delta\n", schema, table)
	return nil
}

// creates the schema which holds the views which aid in querying
// the audit tables for what has changed
func createViewAuditSchema(schema string, db *sql.DB) error {
	query := `DO
		$$
		BEGIN
			IF NOT EXISTS (
					SELECT 1
					FROM information_schema.schemata
					WHERE schema_name = '{{.schema}}_audit'
			) THEN
				CREATE SCHEMA "{{.schema}}_audit";
			END IF;
		END;
		$$
		LANGUAGE plpgsql;`

	data := map[string]interface{}{"schema": schema}

	_, err := db.Exec(mustParseQuery(query, data))
	if err != nil {
		return err
	}

	return nil
}

// returns true if table has only 1 primary key, false if 0 or >1
func hasValidPrimaryKey(schema, table string, db *sql.DB) (bool, error) {
	query := `select coalesce((select format($$'%s'$$, a.attname)
		from pg_attribute a
		join pg_index i on a.attrelid = i.indexrelid
		join pg_class c on i.indrelid = c.oid
		where c.oid = '{{.schema}}.{{.table}}'::regclass
		and i.indisprimary
		and i.indnkeyatts = 1), '') as primary_key`
	data := map[string]interface{}{
		"schema": schema,
		"table":  table,
	}

	rows, err := db.Query(mustParseQuery(query, data))
	if err != nil {
		return false, err
	}
	defer rows.Close()
	var primaryKey string
	for rows.Next() {
		err := rows.Scan(&primaryKey)
		if err != nil {
			return false, err
		}

		if primaryKey == "" {
			log.Printf("SKIPPED table %s.%s due to zero or multi-field PK\n", schema, table)
			return false, nil
		}
	}
	return true, nil
}

// returns a map containing the column name, data type and primary key for
// each column of a given table
func tableColumns(schema, table string, db *sql.DB) ([]map[string]string, error) {
	query := `SELECT DISTINCT ON(attname)
						attname AS column_name,
						format_type(atttypid, atttypmod) AS data_type,
						COALESCE(indisprimary, FALSE) AS primary_key
		FROM pg_attribute
		LEFT JOIN pg_index ON pg_index.indrelid = pg_attribute.attrelid AND pg_attribute.attnum = ANY(pg_index.indkey)
		WHERE pg_attribute.attnum > 0
		AND NOT pg_attribute.attisdropped
		AND pg_attribute.attrelid = '{{.schema}}.{{.table}}'::regclass::oid
		ORDER BY attname, attnum`

	data := map[string]interface{}{
		"schema": schema,
		"table":  table,
	}

	rows, err := db.Query(mustParseQuery(query, data))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var colName, dataType, primaryKey string
	var columns []map[string]string
	for rows.Next() {
		err := rows.Scan(&colName, &dataType, &primaryKey)
		if err != nil {
			return nil, err
		}

		column := make(map[string]string)
		column["colName"] = colName
		column["dataType"] = dataType
		column["primaryKey"] = primaryKey
		columns = append(columns, column)
	}

	return columns, nil
}

// returns the primary key column of a table, given a slice of maps
// where each map represents the information about a given table
// see tableColumns function
func getPrimaryKeyCol(tableCols []map[string]string) map[string]string {
	for _, col := range tableCols {
		if col["primaryKey"] == "true" || col["primaryKey"] == "t" {
			return col
		}
	}

	return nil
}

// creates an audit snapshot view to aid in querying for changes
func createAuditSnapshotView(schema, table, grantee string, tableCols []map[string]string, primaryKeyCol map[string]string, db *sql.DB) error {
	q := `
		DROP VIEW IF EXISTS "{{.schema}}_audit"."{{.table}}_audit_snapshot";
		CREATE VIEW "{{.schema}}_audit"."{{.table}}_audit_snapshot" AS
		SELECT "{{.table}}_audit_id",
						"{{.table}}_audit".primary_key AS primary_key,
						"{{.table}}_audit".changed_at AS audited_changed_at,
						"{{.table}}_audit".operation AS audited_operation,
						"{{.table}}_audit".db_user AS audited_db_user,
						"{{.table}}_audit".changed_by AS audited_change_agent,`

	data := map[string]interface{}{
		"schema":  schema,
		"table":   table,
		"grantee": grantee,
	}

	query := mustParseQuery(q, data)

	for _, col := range tableCols {
		q = `COALESCE((change ->> '{{.colName}}')::{{.dataType}}, COALESCE("{{.colName}}_join".value,`

		if primaryKeyCol != nil {
			q += `("{{.table}}_json" ->> '{{.colName}}')::{{.dataType}}`
		} else {
			q += "NULL"
		}

		q += `)) AS "{{.colName}}",`

		data = map[string]interface{}{
			"schema":   schema,
			"table":    table,
			"colName":  col["colName"],
			"dataType": col["dataType"],
		}

		query += mustParseQuery(q, data)
	}

	query = strings.TrimSuffix(query, ",")

	q = ` FROM "{{.schema}}_audit_raw"."{{.table}}_audit"`

	if primaryKeyCol != nil {
		q += `LEFT JOIN "{{.schema}}"."{{.table}}"
			ON "{{.table}}_audit".primary_key::{{.pkcDataType}} = "{{.schema}}"."{{.table}}"."{{.pkcColName}}"
			LEFT JOIN LATERAL row_to_json("{{.table}}".*) "{{.table}}_json" ON TRUE `
		data["pkcDataType"] = primaryKeyCol["dataType"]
		data["pkcColName"] = primaryKeyCol["colName"]
	}

	query += mustParseQuery(q, data)

	for _, col := range tableCols {
		q = `LEFT JOIN LATERAL (
			SELECT DISTINCT ON(primary_key)
			(before_change ->> '{{.colName}}')::{{.dataType}} AS value
			FROM "{{.schema}}_audit_raw"."{{.table}}_audit" spa
			WHERE (before_change -> '{{.colName}}') IS NOT NULL
			AND spa."{{.table}}_audit_id" > "{{.table}}_audit"."{{.table}}_audit_id"
			AND spa.primary_key = "{{.table}}_audit".primary_key
			ORDER BY spa.primary_key, spa."{{.table}}_audit_id"
			) "{{.colName}}_join" ON TRUE `

		data["colName"] = col["colName"]
		data["dataType"] = col["dataType"]

		query += mustParseQuery(q, data)
	}

	if grantee != "" {
		query += mustParseQuery("; GRANT SELECT ON \"{{.schema}}_audit\".\"{{.table}}_audit_snapshot\" TO "+grantee+"; ", data)
	} else {
		query += "; "
	}

	tx, txErr := db.Begin()
	if txErr != nil {
		return txErr
	}

	_, err := tx.Exec(query)
	if err != nil {
		tx.Rollback()
		log.Println("error occurred while creating snapshot view: ", err)
		errorCounter++
		return nil
	}

	if err = tx.Commit(); err != nil {
		log.Println("error committing delta view transaction: ", err)
		return nil
	}

	log.Printf("created view %s_audit.%s_audit_snapshot\n", schema, table)
	return nil
}

// creates a compare view to aid in querying for changes
func createAuditCompareView(schema, table, grantee string, tableCols []map[string]string, primaryKeyCol map[string]string, db *sql.DB) error {
	q := `
		DROP VIEW IF EXISTS "{{.schema}}_audit"."{{.table}}_audit";
		DROP VIEW IF EXISTS "{{.schema}}_audit"."{{.table}}_audit_compare";
		CREATE VIEW "{{.schema}}_audit"."{{.table}}_audit_compare" AS
		SELECT "{{.table}}_audit_id",
						"{{.table}}_audit".primary_key AS primary_key,
						"{{.table}}_audit".changed_at AS audited_changed_at,
						"{{.table}}_audit".operation AS audited_operation,
						"{{.table}}_audit".db_user AS audited_db_user,
						"{{.table}}_audit".changed_by AS audited_change_agent,`

	data := map[string]interface{}{
		"schema":  schema,
		"table":   table,
		"grantee": grantee,
	}

	query := mustParseQuery(q, data)

	for _, col := range tableCols {
		q = ` COALESCE((before_change ->> '{{.colName}}')::{{.dataType}},
			CASE WHEN "{{.table}}_audit".operation = 'I' THEN NULL ELSE
			COALESCE("{{.colName}}_join".value,`

		if primaryKeyCol != nil {
			q += ` ("{{.table}}_json" ->> '{{.colName}}')::{{.dataType}}`
		} else {
			q += " NULL"
		}

		q += `)
			END) AS "old_{{.colName}}",
			COALESCE((change ->> '{{.colName}}')::{{.dataType}}, COALESCE(
			CASE WHEN "{{.table}}_audit".operation = 'D'
			OR "{{.table}}_audit".operation = 'T' THEN NULL ELSE "{{.colName}}_join".value END,`

		if primaryKeyCol != nil {
			q += `("{{.table}}_json" ->> '{{.colName}}')::{{.dataType}}`
		} else {
			q += "NULL"
		}

		q += `)) AS "new_{{.colName}}",`

		data["colName"] = col["colName"]
		data["dataType"] = col["dataType"]

		query += mustParseQuery(q, data)
	}

	query = strings.TrimSuffix(query, ",")

	q = `FROM "{{.schema}}_audit_raw"."{{.table}}_audit"`

	if primaryKeyCol != nil {
		q += ` LEFT JOIN "{{.schema}}"."{{.table}}" ON "{{.table}}_audit".primary_key::{{.pkcDataType}} = "{{.schema}}"."{{.table}}"."{{.pkcColName}}"
			LEFT JOIN LATERAL row_to_json("{{.table}}".*) "{{.table}}_json" ON TRUE `
	}

	data["pkcDataType"] = primaryKeyCol["dataType"]
	data["pkcColName"] = primaryKeyCol["colName"]

	query += mustParseQuery(q, data)

	for _, col := range tableCols {
		q = ` LEFT JOIN LATERAL (
			SELECT DISTINCT ON(primary_key)
			(before_change ->> '{{.colName}}')::{{.dataType}} AS value
			FROM "{{.schema}}_audit_raw"."{{.table}}_audit" spa
			WHERE (before_change -> '{{.colName}}') IS NOT NULL
			AND spa."{{.table}}_audit_id" > "{{.table}}_audit"."{{.table}}_audit_id"
			AND spa.primary_key = "{{.table}}_audit".primary_key
			ORDER BY spa.primary_key, spa."{{.table}}_audit_id"
			) "{{.colName}}_join" ON TRUE `

		data = map[string]interface{}{
			"schema":   schema,
			"table":    table,
			"colName":  col["colName"],
			"dataType": col["dataType"],
		}

		query += mustParseQuery(q, data)
	}

	if grantee != "" {
		query += mustParseQuery("; GRANT SELECT ON \"{{.schema}}_audit\".\"{{.table}}_audit_compare\" TO "+grantee+"; ", data)
	} else {
		query += "; "
	}
	tx, txErr := db.Begin()
	if txErr != nil {
		return txErr
	}

	_, err := tx.Exec(query)
	if err != nil {
		tx.Rollback()
		log.Println("error occurred while creating compare view: ", err)
		errorCounter++
		return nil
	}

	if err = tx.Commit(); err != nil {
		log.Println("error committing delta view transaction: ", err)
		return nil
	}

	log.Printf("created view %s_audit.%s_audit_compare\n", schema, table)
	return nil
}

func printQueryIfDebug(query string) {
	if os.Getenv("QUERY_DEBUG") == "1" {
		fmt.Println(query)
	}
}
