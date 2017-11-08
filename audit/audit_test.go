package audit

import (
	"database/sql"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var db *sql.DB

func TestMain(m *testing.M) {
	var config Config
	ParseFlags(&config)
	getConfig(&config)

	// Open DB
	db = setupDB(&config)
	defer db.Close()

	// Run Audit_star
	runAllTest()
	retCode := m.Run()
	os.Exit(retCode)
}

// Check config file
func TestGetConfig(t *testing.T) {
	var c Config
	c.CfgPath = "./audit.yml"

	// happy path
	getconfigErr := GetConfig(&c)
	assert.NoError(t, getconfigErr)
	assert.Equal(t, "audit_star", c.DBName)
	assert.Contains(t, c.ExcludedSchemas, "schema_skipme")

	// Test file does not exist
	c = Config{}
	c.CfgPath = "./notexisting_audit.yml"
	getconfigErr = GetConfig(&c)
	assert.Equal(t, "", c.DBName)
	assert.EqualError(t, getconfigErr, "open ./notexisting_audit.yml: no such file or directory")

	//Test incorrect format in yml file throws error
	originalFile, fileErr := ioutil.ReadFile("./audit.yml")
	assert.NoError(t, fileErr)
	defer ioutil.WriteFile("./audit.yml", originalFile, 0644)
	badFileData := originalFile
	badFileData = append(badFileData, []byte("\nincorrect_format=bad_data which should error")...)
	fileErr = ioutil.WriteFile("./audit.yml", badFileData, 0644)
	assert.NoError(t, fileErr)

	c.CfgPath = "./audit.yml"
	getconfigErr = GetConfig(&c)
	assert.Error(t, getconfigErr)
}

func TestTableExclusions(t *testing.T) {
	var c Config
	ParseFlags(&c)
	c.CfgPath = "./audit.yml"
	getConfigErr := GetConfig(&c)
	assert.NoError(t, getConfigErr)

	want := []string{"teststar.table_skipme"}
	assert.Equal(t, want, c.ExcludedTables)

	// query the db to see if the above table has its trigger disabled
	query := `Select count(*) from pg_trigger where tgname='row_audit_star' and`
	query += ` tgenabled = 'D' and tgrelid = 'teststar.table_skipme'::regclass`

	db := setupDB(&c)
	var disabledCount int
	scanErr := db.QueryRow(query).Scan(&disabledCount)
	assert.NoError(t, scanErr)

	// the above query counted how many triggers are enabled for teststar.table_skipme
	// which we disabled in the test config file  audit/audit.yml and we expect
	// the total to be 1
	expected := 1
	assert.Equal(t, expected, disabledCount)
}

func TestSchemaExclusions(t *testing.T) {
	var c Config
	ParseFlags(&c)
	c.CfgPath = "./audit.yml"
	getConfigErr := GetConfig(&c)
	assert.NoError(t, getConfigErr)

	// we assert that the "schema_skipme" is in the arry of ExcludedSchemas as per
	// the audit/audit.yml config file
	assert.Contains(t, c.ExcludedSchemas, "schema_skipme")

	// query the db to see if the table in the schema_skipme schema has its trigger disabled
	query := `Select count(*) from pg_trigger where tgname='row_audit_star' and `
	query += `tgenabled = 'D' and tgrelid = 'schema_skipme.table_skipme2'::regclass`

	db := setupDB(&c)
	var disabledCount int
	scanErr := db.QueryRow(query).Scan(&disabledCount)
	assert.NoError(t, scanErr)

	// the above query counted how many triggers are enabled for schema_skipme.table_skipme2
	// which we disabled in the test config file audit/audit.yml and we expect
	// the total to be 1
	expected := 1
	assert.Equal(t, expected, disabledCount)
}

func TestInsertBehavior(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	// act
	_, insertErr := tx.Exec("insert into teststar.table1 values (1, 'some value');")
	assert.NoError(t, insertErr)

	row := tx.QueryRow("select primary_key, operation, before_change, change, changed_at from teststar_audit_raw.table1_audit where operation = 'I' order by 1 desc limit 1;")

	c := column{}
	scanErr := row.Scan(&c.primaryKey, &c.operation, &c.beforeChange, &c.change, &c.changedAt)
	assert.NoError(t, scanErr)

	// assertions
	assert.Equal(t, 1, int(c.primaryKey.Int64))
	assert.Equal(t, "I", c.operation.String)
	assert.Equal(t, "", c.beforeChange.String)
	assert.Equal(t, "", c.change.String)
	assert.NotEmpty(t, c.changedAt.Time)
}

func TestUpdateBehavior(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	_, insertErr := tx.Exec("insert into teststar.table1 values (1, 'some value');")
	assert.NoError(t, insertErr)

	// act
	_, updateErr := tx.Exec("update teststar.table1 set column2 = 'some other value';")
	assert.NoError(t, updateErr)

	row := tx.QueryRow("select primary_key, operation, before_change, change, changed_at from teststar_audit_raw.table1_audit where operation = 'U' order by 1 desc limit 1;")

	c := column{}
	scanErr := row.Scan(&c.primaryKey, &c.operation, &c.beforeChange, &c.change, &c.changedAt)
	assert.NoError(t, scanErr)

	// assertions
	assert.Equal(t, 1, int(c.primaryKey.Int64))
	assert.Equal(t, "U", c.operation.String)
	assert.Equal(t, `{"column2": "some value"}`, c.beforeChange.String)
	assert.Equal(t, `{"column2": "some other value"}`, c.change.String)
	assert.NotEqual(t, `{"id": "1"}`, c.change.String)
	assert.NotEmpty(t, c.changedAt.Time)
}

func TestDeleteBehavior(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)

	defer tx.Rollback()

	_, insertErr := tx.Exec("insert into teststar.table1 values (1, 'some value');")
	assert.NoError(t, insertErr)

	// act
	_, deleteErr := tx.Exec("delete from teststar.table1 where id = 1;")
	assert.NoError(t, deleteErr)

	row := tx.QueryRow("select primary_key, operation, before_change, change, changed_at from teststar_audit_raw.table1_audit where operation = 'D' order by 1 desc limit 1;")

	c := column{}
	scanErr := row.Scan(&c.primaryKey, &c.operation, &c.beforeChange, &c.change, &c.changedAt)
	assert.NoError(t, scanErr)

	// assertions
	assert.Equal(t, 1, int(c.primaryKey.Int64))
	assert.Equal(t, "D", c.operation.String)
	assert.Equal(t, `{"id": "1", "column2": "some value", "column3": null, "updated_by": null}`, c.beforeChange.String)
	assert.Equal(t, "", c.change.String)
	assert.NotEmpty(t, c.changedAt.Time)
}

func TestTruncateBehavior(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	_, insertErr := tx.Exec("insert into teststar.table1 values (1, 'some value');")
	assert.NoError(t, insertErr)

	// act
	_, truncateErr := tx.Exec("Truncate Table teststar.table1;")
	assert.NoError(t, truncateErr)

	row := tx.QueryRow("select operation, primary_key, before_change, change, changed_at from teststar_audit_raw.table1_audit where operation = 'T' order by 1 desc limit 1;")

	c := column{}
	scanErr := row.Scan(&c.operation, &c.primaryKey, &c.beforeChange, &c.change, &c.changedAt)
	assert.NoError(t, scanErr)

	// assertions
	assert.Equal(t, "T", c.operation.String)
	assert.Equal(t, 0, int(c.primaryKey.Int64))
	assert.Equal(t, "", c.beforeChange.String)
	assert.Equal(t, "", c.change.String)
	assert.NotEmpty(t, c.changedAt.Time)

}

func TestCompoundPrimaryKeyInsert(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	// act
	_, insertErr := tx.Exec("insert into teststar.table2 values (1, 1, 'some value');")
	assert.NoError(t, insertErr)

	row := tx.QueryRow("select primary_key, operation, before_change, change, changed_at from teststar_audit_raw.table2_audit where operation = 'I' order by 1 desc limit 1;")

	c := column{}
	scanErr := row.Scan(&c.primaryKey, &c.operation, &c.beforeChange, &c.change, &c.changedAt)
	assert.NoError(t, scanErr)

	// assertions
	assert.Equal(t, 0, int(c.primaryKey.Int64))
	assert.Equal(t, "I", c.operation.String)
	assert.Equal(t, "", c.beforeChange.String)
	assert.Equal(t, "", c.change.String)
	assert.NotEmpty(t, c.changedAt.Time)

}

func TestCompoundPrimaryKeyUpdate(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	// insert record
	_, insertErr := tx.Exec("insert into teststar.table2 values (1, 1, 'some value');")
	assert.NoError(t, insertErr)

	// act
	_, updateErr := tx.Exec("update teststar.table2 set column3 = 'some other value';")
	assert.NoError(t, updateErr)

	row := tx.QueryRow("select primary_key, operation, before_change, change, changed_at from teststar_audit_raw.table2_audit where operation = 'U' order by 1 desc limit 1;")

	c := column{}
	scanErr := row.Scan(&c.primaryKey, &c.operation, &c.beforeChange, &c.change, &c.changedAt)
	assert.NoError(t, scanErr)

	// assertions
	assert.Equal(t, 0, int(c.primaryKey.Int64))
	assert.Equal(t, "U", c.operation.String)
	assert.Equal(t, `{"column3": "some value"}`, c.beforeChange.String)
	assert.Equal(t, `{"column3": "some other value"}`, c.change.String)
	assert.NotEqual(t, `{"id": "1"}`, c.change.String)
	assert.NotEmpty(t, c.changedAt.Time)
}

func TestCompoundPrimaryKeyDelete(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	// insert record
	_, insertErr := tx.Exec("insert into teststar.table2 values (1, 1, 'some value');")
	assert.NoError(t, insertErr)

	// act
	_, deleteErr := tx.Exec("delete from teststar.table2 where id = 1;")
	assert.NoError(t, deleteErr)

	row := tx.QueryRow("select primary_key, operation, before_change, change, changed_at from teststar_audit_raw.table2_audit where operation = 'D' order by 1 desc limit 1;")

	c := column{}
	scanErr := row.Scan(&c.primaryKey, &c.operation, &c.beforeChange, &c.change, &c.changedAt)
	assert.NoError(t, scanErr)

	// assertions
	assert.Equal(t, 0, int(c.primaryKey.Int64))
	assert.Equal(t, "D", c.operation.String)
	assert.Equal(t, `{"id": "1", "id2": "1", "column3": "some value", "updated_by": null}`, c.beforeChange.String)
	assert.Equal(t, "", c.change.String)
	assert.NotEmpty(t, c.changedAt.Time)
}

func TestViewsInsert(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	// act
	_, insertErr := tx.Exec("insert into teststar.table1 values (1, 'some value');")
	assert.NoError(t, insertErr)

	row := tx.QueryRow("select old_id, new_id, old_column2, new_column2 from teststar_audit.table1_audit_delta where primary_key = '1' and audited_operation = 'I';")

	c := column{}
	scanErr := row.Scan(&c.oldID, &c.newID, &c.oldColumn2, &c.newColumn2)
	assert.NoError(t, scanErr)

	// assertions
	// shows only changed fields in the delta view
	assert.Equal(t, 0, int(c.oldID.Int64))
	assert.Equal(t, 1, int(c.newID.Int64))
	assert.Equal(t, "", c.oldColumn2.String)
	assert.Equal(t, "some value", c.newColumn2.String)
}

func TestViewsUpdate(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	_, insertErr := tx.Exec("insert into teststar.table1 values (1, 'some value');")
	assert.NoError(t, insertErr)

	// act
	_, updateErr := tx.Exec("update teststar.table1 set column2 = 'some other value' where id = 1;")
	assert.NoError(t, updateErr)

	rowUpdated := tx.QueryRow("select old_id, new_id, old_column2, new_column2 from teststar_audit.table1_audit_delta where primary_key = '1' and audited_operation = 'U';")

	c := column{}
	scanUpdatedErr := rowUpdated.Scan(&c.oldID, &c.newID, &c.oldColumn2, &c.newColumn2)
	assert.NoError(t, scanUpdatedErr)

	// assertions
	// shows only changed fields in the delta view
	assert.Equal(t, 0, int(c.oldID.Int64))
	assert.Equal(t, 0, int(c.newID.Int64))
	assert.Equal(t, "some value", c.oldColumn2.String)
	assert.Equal(t, "some other value", c.newColumn2.String)
}

func TestViewsDelete(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	_, insertErr := tx.Exec("insert into teststar.table1 values (1, 'some value');")
	assert.NoError(t, insertErr)

	// act
	_, deleteErr := tx.Exec("delete from teststar.table1 where id = 1;")
	assert.NoError(t, deleteErr)

	row := tx.QueryRow("select old_id, new_id, old_column2, new_column2 from teststar_audit.table1_audit_delta where primary_key = '1' and audited_operation = 'D';")

	c := column{}
	scanErr := row.Scan(&c.oldID, &c.newID, &c.oldColumn2, &c.newColumn2)
	assert.NoError(t, scanErr)

	// assertions
	// shows only changed fields in the delta view
	assert.Equal(t, 1, int(c.oldID.Int64))
	assert.Equal(t, 0, int(c.newID.Int64))
	assert.Equal(t, "some value", c.oldColumn2.String)
	assert.Equal(t, "", c.newColumn2.String)
}

func TestSnapshotViewInsert(t *testing.T) {
	//arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	// act
	_, insertErr := tx.Exec("insert into teststar.table1 values (2, 'some value');")
	assert.NoError(t, insertErr)

	row := tx.QueryRow("select id, column2 from teststar_audit.table1_audit_snapshot where primary_key = '2' and audited_operation = 'I';")

	c := column{}
	errScan := row.Scan(&c.id, &c.column2)
	assert.NoError(t, errScan)

	// assertions
	assert.Equal(t, 2, int(c.id.Int64))
	assert.Equal(t, "some value", c.column2.String)

}

func TestSnapshotViewUpdate(t *testing.T) {
	//arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	_, insertErr := tx.Exec("insert into teststar.table1 values (2, 'some value');")
	assert.NoError(t, insertErr)

	_, updateErr := tx.Exec("update teststar.table1 set column2 = 'some other value' where id = 2;")
	assert.NoError(t, updateErr)

	row := tx.QueryRow("select id, column2 from teststar_audit.table1_audit_snapshot where primary_key = '2' and audited_operation = 'U';")

	c := column{}
	errScan := row.Scan(&c.id, &c.column2)
	assert.NoError(t, errScan)

	// assertions
	assert.Equal(t, 2, int(c.id.Int64))
	assert.Equal(t, "some other value", c.column2.String)

}

func TestSnapshotViewDelete(t *testing.T) {
	//arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	_, insertErr := tx.Exec("insert into teststar.table1 values (2, 'some value');")
	assert.NoError(t, insertErr)

	_, updateErr := tx.Exec("update teststar.table1 set column2 = 'some other value' where id = 2;")
	assert.NoError(t, updateErr)

	_, deleteErr := tx.Exec("delete from teststar.table1 where id = 2;")
	assert.NoError(t, deleteErr)

	row := tx.QueryRow("select id, column2 from teststar_audit.table1_audit_snapshot where primary_key = '2' and audited_operation = 'D';")

	c := column{}
	errScan := row.Scan(&c.id, &c.column2)
	assert.NoError(t, errScan)

	// assertions
	assert.Equal(t, 0, int(c.id.Int64))
	assert.Equal(t, "", c.column2.String)
}

func TestCompareViewInsert(t *testing.T) {
	//arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	_, insertErr := tx.Exec("insert into teststar.table1 values (3, 'some value');")
	assert.NoError(t, insertErr)

	row := tx.QueryRow("select old_id, new_id, old_column2, new_column2 from teststar_audit.table1_audit_compare where primary_key = '3' and audited_operation = 'I';")

	c := column{}
	scanErr := row.Scan(&c.oldID, &c.newID, &c.oldColumn2, &c.newColumn2)
	assert.NoError(t, scanErr)

	// assertions
	// shows only changed fields in the delta view
	assert.Equal(t, 0, int(c.oldID.Int64))
	assert.Equal(t, 3, int(c.newID.Int64))
	assert.Equal(t, "", c.oldColumn2.String)
	assert.Equal(t, "some value", c.newColumn2.String)
}

func TestCompareViewUpdate(t *testing.T) {
	//arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	_, insertErr := tx.Exec("insert into teststar.table1 values (3, 'some value');")
	assert.NoError(t, insertErr)

	_, updateErr := tx.Exec("update teststar.table1 set column2 = 'some other value' where id = 3;")
	assert.NoError(t, updateErr)

	row := tx.QueryRow("select old_id, new_id, old_column2, new_column2 from teststar_audit.table1_audit_compare where primary_key = '3' and audited_operation = 'U';")

	c := column{}
	scanErr := row.Scan(&c.oldID, &c.newID, &c.oldColumn2, &c.newColumn2)
	assert.NoError(t, scanErr)

	// assertions
	// shows only changed fields in the delta view
	assert.Equal(t, 3, int(c.oldID.Int64))
	assert.Equal(t, 3, int(c.newID.Int64))
	assert.Equal(t, "some value", c.oldColumn2.String)
	assert.Equal(t, "some other value", c.newColumn2.String)
}

func TestCompareViewDelete(t *testing.T) {
	//arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	_, insertErr := tx.Exec("insert into teststar.table1 values (3, 'some value');")
	assert.NoError(t, insertErr)

	_, deleteErr := tx.Exec("delete from teststar.table1 where id = 3;")
	assert.NoError(t, deleteErr)

	row := tx.QueryRow("select old_id, new_id, old_column2, new_column2 from teststar_audit.table1_audit_compare where primary_key = '3' and audited_operation = 'D';")

	c := column{}
	scanErr := row.Scan(&c.oldID, &c.newID, &c.oldColumn2, &c.newColumn2)
	assert.NoError(t, scanErr)

	// assertions
	// shows only changed fields in the delta view
	assert.Equal(t, 3, int(c.oldID.Int64))
	assert.Equal(t, 0, int(c.newID.Int64))
	assert.Equal(t, "some value", c.oldColumn2.String)
	assert.Equal(t, "", c.newColumn2.String)
}

func TestSchemaTypeChange(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	// act
	_, alterErr := tx.Exec("alter table teststar.table1 alter column column3 type numeric(9,3);")
	assert.NoError(t, alterErr)

	// assertion
	_, insertErr := tx.Exec("insert into teststar.table1(id, column3) values(4, 9.5);")
	assert.NoError(t, insertErr)

	row := tx.QueryRow("select new_column3 from teststar_audit.table1_audit_compare where primary_key = '4';")

	c := column{}
	scanErr := row.Scan(&c.newColumn3)
	assert.NoError(t, scanErr)
	assert.Equal(t, 9.50, c.newColumn3.Float64)
}

func TestSchemaNameChange(t *testing.T) {
	tests := []struct {
		query    string
		expected bool
	}{
		{query: "alter table teststar.table1 rename column column3 to 1;", expected: true},
		{query: "alter table teststar.table1 rename column column3 to column4&;", expected: true},
		{query: "alter table teststar.table1 rename column column3 to column 4;", expected: true},
		{query: "alter table teststar.table1 rename column column3 to @#;", expected: true},
	}

	// arrangement
	for _, test := range tests {
		tx, txErr := db.Begin()
		assert.NoError(t, txErr)
		defer tx.Rollback()

		// Check no error happy path
		_, actualErr := tx.Exec(" alter table teststar.table1 rename column column3 to column4;")
		assert.NoError(t, actualErr)

		// act
		// Check Errors sad path
		_, actualErr2 := tx.Exec(test.query)
		actual := assert.Error(t, actualErr2)
		assert.Equal(t, test.expected, actual)
	}

}

func TestAddColumns(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	// act
	// assertion
	_, alterErr := tx.Exec("alter table teststar.table1 add column column4 integer;")
	assert.NoError(t, alterErr)

}

func TestDropColumns(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	// act
	_, alterErr := tx.Exec(" alter table teststar.table1 drop column column3;")

	// assertion
	assert.NoError(t, alterErr)

}

func TestLoggingChangedByInsert(t *testing.T) {
	tests := []struct {
		query    string
		expected string
	}{
		{query: "SET LOCAL audit_star.changed_by TO sr; insert into teststar.table1 values (1, 'some value');", expected: "sr"},
		{query: "insert into teststar.table1 values (2, 'some value');", expected: "default"},
	}

	// arrangement
	for _, test := range tests {
		tx, txErr := db.Begin()
		assert.NoError(t, txErr)
		defer tx.Rollback()

		// act
		_, insertErr := tx.Exec(test.query)
		assert.NoError(t, insertErr)

		row := tx.QueryRow("select changed_by from teststar_audit_raw.table1_audit where operation = 'I' order by 1 desc limit 1;")

		c := column{}
		scanErr := row.Scan(&c.changedBy)
		assert.NoError(t, scanErr)

		// assertion
		assert.Equal(t, test.expected, c.changedBy.String)

	}
}

func TestLoggingChangedByUpdate(t *testing.T) {
	const update = "update teststar.table1 set column2 = 'some other value';"

	tests := []struct {
		queryInsert string
		queryUpdate string
		expected    string
	}{
		{queryInsert: "SET LOCAL audit_star.changed_by TO sr; insert into teststar.table1 values (1, 'some value');", queryUpdate: update, expected: "sr"},
		{queryInsert: "insert into teststar.table1 values (2, 'some value');", queryUpdate: update, expected: "default"},
	}

	// arrangement
	for _, test := range tests {
		tx, txErr := db.Begin()
		assert.NoError(t, txErr)
		defer tx.Rollback()

		// act
		_, insertErr := tx.Exec(test.queryInsert)
		assert.NoError(t, insertErr)

		_, updateErr := tx.Exec(test.queryUpdate)
		assert.NoError(t, updateErr)

		row := tx.QueryRow("select changed_by from teststar_audit_raw.table1_audit where operation = 'U' order by 1 desc limit 1;")

		c := column{}
		scanErr := row.Scan(&c.changedBy)
		assert.NoError(t, scanErr)

		// assertion
		assert.Equal(t, test.expected, c.changedBy.String)

	}
}

func TestLoggingChangedByDelete(t *testing.T) {
	tests := []struct {
		queryInsert string
		queryDelete string
		expected    string
	}{
		{queryInsert: "SET LOCAL audit_star.changed_by TO sr; insert into teststar.table1 values (1, 'some value');", queryDelete: "delete from teststar.table1 where id = 1;", expected: "sr"},
		{queryInsert: "insert into teststar.table1 values (2, 'some value');", queryDelete: "delete from teststar.table1 where id = 2;", expected: "default"},
	}

	// arrangement
	for _, test := range tests {
		tx, txErr := db.Begin()
		assert.NoError(t, txErr)
		defer tx.Rollback()

		// act
		_, insertErr := tx.Exec(test.queryInsert)
		assert.NoError(t, insertErr)

		_, deleteErr := tx.Exec(test.queryDelete)
		assert.NoError(t, deleteErr)

		row := tx.QueryRow("select changed_by from teststar_audit_raw.table1_audit where operation = 'I' order by 1 desc limit 1;")

		c := column{}
		scanErr := row.Scan(&c.changedBy)
		assert.NoError(t, scanErr)

		// assertion
		assert.Equal(t, test.expected, c.changedBy.String)

	}
}

func TestLoggingChangedByTruncate(t *testing.T) {
	const insert = "SET LOCAL audit_star.changed_by TO sr; insert into teststar.table1 values (1, 'some value');"
	const queryRowTable1 = "select changed_by from teststar_audit_raw.table1_audit where operation = 'T' order by 1 desc limit 1;"
	const queryRowTable2 = "select changed_by from teststar_audit_raw.table3_audit where operation = 'T' order by 1 desc limit 1;"

	// arrangement
	tests := []struct {
		queryInsert   string
		queryTruncate string
		queryRow      string
		expected      string
	}{
		{queryInsert: insert, queryTruncate: "truncate table teststar.table1;", queryRow: queryRowTable1, expected: "sr"},
		{queryInsert: "insert into teststar.table3 values (1, 'some value');", queryTruncate: "truncate table teststar.table3;", queryRow: queryRowTable2, expected: "default"},
	}

	for _, test := range tests {
		tx, txErr := db.Begin()
		assert.NoError(t, txErr)
		defer tx.Rollback()

		// act
		_, insertErr := tx.Exec(test.queryInsert)
		assert.NoError(t, insertErr)

		_, truncateErr := tx.Exec(test.queryTruncate)
		assert.NoError(t, truncateErr)

		row := tx.QueryRow(test.queryRow)

		c := column{}
		scanErr := row.Scan(&c.changedBy)
		assert.NoError(t, scanErr)

		// assertion
		assert.Equal(t, test.expected, c.changedBy.String)

	}
}

// Check audit star does not log client_query by default
func TestClientQueryDefault(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	// act
	_, insertErr := tx.Exec("insert into teststar.table2 values (1, 1, 'some value');")
	assert.NoError(t, insertErr)

	_, deleteErr := tx.Exec("delete from teststar.table2 where id = 1;")
	assert.NoError(t, deleteErr)

	row := tx.QueryRow("select client_query from teststar_audit_raw.table2_audit order by 1 desc limit 1;")

	// assertion
	c := column{}
	scanErr := row.Scan(&c.clientQuery)
	assert.NoError(t, scanErr)
	assert.Equal(t, "", c.clientQuery.String)

}

// Check Audit star logs client_query
func TestClientQuery(t *testing.T) {
	// arrangement
	var config Config
	ParseFlags(&config)
	getConfig(&config)

	// Open DB
	db := setupDB(&config)
	defer db.Close()

	config.LogClientQuery = true

	errRun := RunAll(db, &config)
	assert.NoError(t, errRun)

	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	// act
	_, insertErr := tx.Exec("insert into teststar.table2 values (1, 1, 'some value');")
	assert.NoError(t, insertErr)

	_, deleteErr := tx.Exec("delete from teststar.table2 where id = 1;")
	assert.NoError(t, deleteErr)

	row := tx.QueryRow("select client_query from teststar_audit_raw.table2_audit order by 1 desc limit 1;")

	// assertion
	c := column{}
	scanErr := row.Scan(&c.clientQuery)
	assert.NoError(t, scanErr)
	assert.Equal(t, "insert into teststar.table2 values (1, 1, 'some value');", c.clientQuery.String)

}

// Check the count of audit tables created matches table count in target schema
func TestAuditTableCount(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	// act
	tableRow := tx.QueryRow(`select count(*)
	from pg_class
	join pg_roles on pg_class.relowner = pg_roles.oid
	join pg_namespace on pg_namespace.oid = pg_class.relnamespace
	where relkind = 'r'
	and nspname = 'teststar'
	and rolname = 'test__owner';`)

	c := column{}
	tableRowErr := tableRow.Scan(&c.count)
	assert.NoError(t, tableRowErr)

	AuditTableCount := tx.QueryRow(`select count(*)
	from pg_class
	join pg_roles on pg_class.relowner = pg_roles.oid
	join pg_namespace on pg_namespace.oid = pg_class.relnamespace
	and relkind = 'r'
	and nspname = 'teststar_audit_raw'
	and relname like '%_audit';`)

	col := column{}
	auditTableRowErr := AuditTableCount.Scan(&col.count)
	assert.NoError(t, auditTableRowErr)

	// assertion
	assert.Equal(t, int(c.count.Int64), int(col.count.Int64))
}

func TestAuditTableMissingColumn(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)
	defer tx.Rollback()

	// act
	tableRow := tx.QueryRow(` Select count(*) from Information_schema.columns where
	table_catalog = 'audit_star'
	and table_schema = 'teststar'
	and table_name = 'table3'
	and column_name = 'updated_by';`)

	c := column{}
	tableRowErr := tableRow.Scan(&c.count)
	assert.NoError(t, tableRowErr)
	assert.Equal(t, 1, int(c.count.Int64))
}

func TestQuoting(t *testing.T) {

	const specialCharactersColumnNames = `SELECT EXISTS (
            SELECT 1
            FROM pg_trigger
            JOIN pg_class ON tgrelid = pg_class.oid
            JOIN pg_roles ON relowner = pg_roles.oid
            WHERE tgname = 'row_audit_star'
            AND tgrelid = 'teststar_quote.table5'::regclass
            AND rolname = 'test__owner'
          ) AS exists`
	const specialCharactersTableNames = `SELECT EXISTS (
            SELECT 1
            FROM pg_trigger
            JOIN pg_class ON tgrelid = pg_class.oid
            JOIN pg_roles ON relowner = pg_roles.oid
            WHERE tgname = 'row_audit_star'
            AND tgrelid = 'teststar_quote.table_:six'::regclass
            AND rolname = 'test__owner'
          ) AS exists`
	const specialCharactersSchamesName = `SELECT EXISTS (
            SELECT 1
            FROM pg_trigger
            JOIN pg_class ON tgrelid = pg_class.oid
            JOIN pg_roles ON relowner = pg_roles.oid
            WHERE tgname = 'row_audit_star'
            AND tgrelid = 'test:star.table1'::regclass
            AND rolname = 'test__owner'
          ) AS exists`

	const specialCharactersPKColumn = `SELECT EXISTS (
            SELECT 1
            FROM pg_trigger
            JOIN pg_class ON tgrelid = pg_class.oid
            JOIN pg_roles ON relowner = pg_roles.oid
            WHERE tgname = 'row_audit_star'
            AND tgrelid = 'teststar_quote.table7'::regclass
            AND rolname = 'test__owner'
          ) AS exists`

	tests := []struct {
		query    string
		expected string
	}{
		{query: specialCharactersColumnNames, expected: "true"},
		{query: specialCharactersTableNames, expected: "true"},
		{query: specialCharactersSchamesName, expected: "true"},
		{query: specialCharactersPKColumn, expected: "true"},
	}

	for _, test := range tests {

		// arrangement
		tx, txErr := db.Begin()
		assert.NoError(t, txErr)

		defer tx.Rollback()

		// act
		row := tx.QueryRow(test.query)

		// assertion
		c := column{}
		scanErr := row.Scan(&c.exists)
		assert.NoError(t, scanErr)
		assert.Equal(t, test.expected, c.exists.String)
	}
}

func TestSpecialCharactersOwner(t *testing.T) {
	// arrangement
	var config Config
	ParseFlags(&config)
	getConfig(&config)

	config.Owner = "7357:owner"

	// Open DB
	db := setupDB(&config)
	defer db.Close()

	errRun := RunAll(db, &config)
	assert.NoError(t, errRun)

	tx, txErr := db.Begin()
	assert.NoError(t, txErr)

	defer tx.Rollback()

	row := tx.QueryRow(`SELECT EXISTS (
			SELECT 1
			FROM pg_trigger
			JOIN pg_class ON tgrelid = pg_class.oid
			JOIN pg_roles ON relowner = pg_roles.oid
			WHERE tgname = 'row_audit_star'
			AND tgrelid = 'teststar_quote.table8'::regclass
			AND rolname = '7357:owner'
			) AS exists`)

	c := column{}
	scanErr := row.Scan(&c.exists)
	assert.NoError(t, scanErr)
	assert.Equal(t, "true", c.exists.String)

}

func TestSecurityDefinerDefault(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)

	// act
	row := tx.QueryRow(`SELECT NOT EXISTS (
			SELECT 1
			FROM pg_class
			JOIN pg_trigger ON pg_trigger.tgrelid = pg_class.oid
			JOIN pg_proc ON pg_proc.oid = pg_trigger.tgfoid
			JOIN pg_roles ON pg_roles.oid = pg_class.relowner
			WHERE proname LIKE 'audit%'
			AND NOT prosecdef
			AND rolname = 'test__owner'
			) AS exists`)

	// assertion
	c := column{}
	scanErr := row.Scan(&c.exists)
	assert.NoError(t, scanErr)
	assert.Equal(t, "true", c.exists.String)
}

func TestSecurityInvoker(t *testing.T) {
	// arrangement
	var config Config
	ParseFlags(&config)
	getConfig(&config)

	config.Security = "invoker"

	// Open DB
	db := setupDB(&config)
	defer db.Close()

	errRun := RunAll(db, &config)
	assert.NoError(t, errRun)

	tx, txErr := db.Begin()
	assert.NoError(t, txErr)

	defer tx.Rollback()

	// act
	row := tx.QueryRow(`SELECT NOT EXISTS (
			SELECT 1
			FROM pg_class
			JOIN pg_trigger ON pg_trigger.tgrelid = pg_class.oid
			JOIN pg_proc ON pg_proc.oid = pg_trigger.tgfoid
			JOIN pg_roles ON pg_roles.oid = pg_class.relowner
			WHERE proname LIKE 'audit%'
			AND prosecdef
			AND rolname = 'test__owner'
			) AS exists`)

	// assertion
	c := column{}
	scanErr := row.Scan(&c.exists)
	assert.NoError(t, scanErr)
	assert.Equal(t, "true", c.exists.String)

}

func TestInternalPostgresSchemas(t *testing.T) {
	//arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)

	defer tx.Rollback()

	// act
	tableRow := tx.QueryRow(` select count(*)
        from pg_class
        join pg_namespace on pg_namespace.oid = pg_class.relnamespace
        and relkind = 'r'
        and nspname LIKE 'pg\\_%'
        and relname like '%_audit';`)

	c := column{}
	tableRowErr := tableRow.Scan(&c.count)
	assert.NoError(t, tableRowErr)
	assert.Equal(t, 0, int(c.count.Int64))
}

func TestAuditPgs(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)

	defer tx.Rollback()

	// act
	tableRow := tx.QueryRow(` select count(*)
				from pg_class
				join pg_namespace on pg_namespace.oid = pg_class.relnamespace
				and relkind = 'r'
				and nspname = 'pgs_audit_raw'
				and relname like '%_audit';`)

	// assetrions
	c := column{}
	tableRowErr := tableRow.Scan(&c.count)
	assert.NoError(t, tableRowErr)
	assert.Equal(t, 1, int(c.count.Int64))
}

// check if audit_star log parsed time about 1000 entries
func TestLogParsedtime(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)

	defer tx.Rollback()

	_, insertErr := tx.Exec("insert into teststar.table2 values (1, 1, 'some value');")
	assert.NoError(t, insertErr)

	_, execErr := tx.Exec(`DO
        $$
        DECLARE
          i INTEGER;
        BEGIN
          FOR i IN 1..1010 LOOP
            UPDATE teststar.table2 SET id = id;
          END LOOP;
        END;
        $$
        LANGUAGE plpgsql;`)
	assert.NoError(t, execErr)

	// act
	_, truncateErr := tx.Exec("Truncate teststar.table2;")
	assert.NoError(t, truncateErr)

	row := tx.QueryRow(`SELECT EXISTS (
          SELECT 1
          FROM teststar_audit_raw.table2_audit
          WHERE sparse_time IS NOT NULL
        ) AS exists`)

	// assertion
	c := column{}
	scanErr := row.Scan(&c.exists)
	assert.NoError(t, scanErr)
	assert.Equal(t, "true", c.exists.String)
}

func TestAuditTablesDefaultOwner(t *testing.T) {
	// arrangement
	tx, txErr := db.Begin()
	assert.NoError(t, txErr)

	defer tx.Rollback()

	// act
	row := tx.QueryRow(`SELECT EXISTS (
				SELECT 1
				FROM pg_trigger
				JOIN pg_class ON tgrelid = pg_class.oid
				JOIN pg_roles ON relowner = pg_roles.oid
				WHERE tgname = 'row_audit_star'
				AND tgrelid = 'teststar_2.table2'::regclass
				AND rolname = 'test__owner'
				) AS exists`)

	// assertion
	c := column{}
	scanErr := row.Scan(&c.exists)
	assert.NoError(t, scanErr)
	assert.Equal(t, "true", c.exists.String)
}

func TestAuditTablesOwnerSpecified(t *testing.T) {
	// arrangement
	var config Config
	ParseFlags(&config)
	getConfig(&config)

	config.Owner = "not_test__owner"

	// Open DB
	db := setupDB(&config)
	defer db.Close()

	errRun := RunAll(db, &config)
	assert.NoError(t, errRun)

	tx, txErr := db.Begin()
	assert.NoError(t, txErr)

	defer tx.Rollback()

	// act
	row := tx.QueryRow(`SELECT EXISTS (
			SELECT 1
			FROM pg_trigger
			JOIN pg_class ON tgrelid = pg_class.oid
			JOIN pg_roles ON relowner = pg_roles.oid
			JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
			WHERE tgname = 'row_audit_star'
			AND rolname = 'not_test__owner'
			AND nspname IN ('teststar', 'teststar_2', 'teststar_3', 'schema_skipme', 'accounting')
			) AS exists`)

	// assertion
	c := column{}
	scanErr := row.Scan(&c.exists)
	assert.NoError(t, scanErr)
	assert.Equal(t, "true", c.exists.String)
}

func TestAuditTablesOwnerNotSpecified(t *testing.T) {
	// arrangement
	var config Config
	ParseFlags(&config)
	getConfig(&config)

	// owner set to nil
	config.Owner = ""

	// Open DB
	db := setupDB(&config)
	defer db.Close()

	errRun := RunAll(db, &config)
	assert.NoError(t, errRun)

	tx, txErr := db.Begin()
	assert.NoError(t, txErr)

	defer tx.Rollback()

	// act
	row := tx.QueryRow(`SELECT array_agg(DISTINCT(rolname))::TEXT[] <@ ARRAY['test__owner', 'not_test__owner', 'definitely_not_test__owner', '7357:owner']
			AND array_agg(DISTINCT(rolname))::TEXT[] @> ARRAY['test__owner', 'not_test__owner', 'definitely_not_test__owner', '7357:owner'] AS check
			FROM pg_trigger
			JOIN pg_class ON tgrelid = pg_class.oid
			JOIN pg_roles ON relowner = pg_roles.oid
			JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
			WHERE tgname = 'row_audit_star'
			AND nspname IN ('teststar', 'teststar_2', 'teststar_3', 'schema_skipme', 'teststar_quote');`)

	// assertion
	c := column{}
	scanErr := row.Scan(&c.exists)
	assert.NoError(t, scanErr)
	assert.Equal(t, "true", c.exists.String)
}

func TestSchemaNotOwnedByConfigOwner(t *testing.T) {
	// arrangement
	var config Config
	ParseFlags(&config)
	getConfig(&config)

	// make sure owner is test__owner
	config.Owner = "test__owner"

	// Open DB
	db := setupDB(&config)
	defer db.Close()

	errRun := RunAll(db, &config)
	assert.NoError(t, errRun)

	tx, txErr := db.Begin()
	assert.NoError(t, txErr)

	defer tx.Rollback()

	row := tx.QueryRow(`SELECT EXISTS (
		SELECT schema_name
		FROM information_schema.schemata
		WHERE schema_name = 'teststar3_audit'
	) AS exists`)

	c := column{}
	scanErr := row.Scan(&c.exists)
	assert.NoError(t, scanErr)
	assert.Equal(t, "false", c.exists.String)
}
