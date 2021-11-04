/*

Purpose

Audit_Star is a database building block that is intended to provide a standard, easy to use mechanism for capturing row-level data changes in your Postgres databases.  It is to provide support for those scenarios where we do not already have another auditing mechanism in place (such as historian functionality or special application level events).

Benefits

The general benefits of auditing are well known.  Aside from capturing a historical perspective of the data, it also provides a mechanism to assist in troubleshooting/debugging certain types of problems/issues (particularly those that can be tied to recent data changes).  A secondary benefit includes providing a mechanism to communicate to ETL routines or downstream systems that a change (specifically a "delete") has taken place.

Some of the benefits that are specific to using this particular approach include the following:

- There is no maintenance required
- There is no custom code required
- The auditing mechanism never breaks, even when columns are added or removed from a table
- It is easy to apply/deploy
- It can be configured/customized (if needed) via a simple config file

How Does it Work?

This auditing mechanism involves (automatically) generating a number of database components for each table that is to be audited:

- Audit Table - Each "source" table will have a corresponding audit table (to hold the RECENT history of changes)
- Audit Trigger - Each source table will have a DML Trigger that propagates the data changes to its audit table
- Audit View - Each table will have a View that aids in querying/presenting the (JSON) data contained in the audit table

The schema of the audit tables do NOT match/mirror the source table they are auditing.  Rather, the audit tables have a single column that holds a JSON representation of the entire "before" row, and a second column that holds a JSON representation of the diff between the "before" and "after" rows.  The audit trigger handles creating the JSON representation of the changes, and inserting it into the audit table.  The trigger also handle creating new audit table partitions as needed.  The audit view converts the JSON back to a row-based representation of the data changes, so that the changes are easier to review.  The view also stitches together certain data from the source table (as in the case of initial inserts).

Deployment


HStore Extension

The auditing solution is built on top of the HStore data type in Postgres.  Hence, the HStore Extension must be installed in the database.


    CREATE EXTENSION hstore with schema public;


Be sure that the extension is bubbled down to staging and development environments.  Otherwise the migrations builds/tests will fail.

Run time parameters

The auditing solution uses PostgreSQL runtime parameters to pass metadata such as who made the change from the app to the audit system.  The parameters currently used now are `audit_star.changed_by` and `audit_star.changed_reason`. These are defaults that must be set at the database level, usually to an empty string.


  ALTER DATABASE <your-db-name> SET audit_star.changed_by TO '';
  ALTER DATABASE <your-db-name> SET audit_star.change_reason TO '';


Be sure that the setting is bubbled down to staging and development environments.  Otherwise the migrations builds/tests will fail.

Before using audit_star, database-specific configuration must be made to the ```audit.yml```
file.  By default, audit_star will look in the current directory from which it is
being executed, but the optional parameter ```-cfg``` allows the user to provide
an alternative path to the ```audit.yml``` file.

The database-specific configuration along with accepted values are detailed in the
example file provided in `audit.yml` (copied below).


     database config information
     host: database host name
     post: database port number; postgres default is 5432
     db_name: database_name (name of the db to audit)
     username: database username used to connect
     password: databsase password used to connect

     audit star config information
     excluded_tables:
       - exclude this_schema.this_table
     excluded_schemas:
       - exclude_this_schema
     owner: app__owner (only audit tables owned by this user, if not specified will audit *every* table it can)
     log_client_query: false (toggle logging of query that caused the change)
     security: definer/invoker (security level of audit function - usually definer on release to avoid race conditions with defining permissions)


Installation

The preferred way to deploy Go binaries is using `go get` and `go install`.
Optionally, you can clone the repo manually into your $GOPATH and run `go build`.

Since audit_star is written in [Go](https://golang.org/), in order to build audit_star you must have Go
installed on your machine.  Alternatively, you can build the binary on one machine
and then copy the binary to a machine, as needed, before invoking it, assuming the
machines share the same architecture.  Go also supports [cross-compilation](https://dave.cheney.net/2012/09/08/an-introduction-to-cross-compilation-with-go)
which allows you to build the binary on a machine with one architecture and run
it on a machine with a completely different architecture.

 Using `go get` (preferred)
You can download audit_star by running `go get github.com/enova/audit_star`.
This will download audit_star into your $GOPATH and install the binary into your $GOPATH/bin folder.
If your $GOPATH/bin folder is in your $PATH then you can run audit_star from anywhere by executing `audit_star`.

 Building with ```go build```
After cloning the git repo, ```cd``` into the directory and run ```go build```.  Assuming your Go is set up properly, this will create an ```audit_star``` binary in the current directly.  You can then execute this binary by executing ```./audit_star```.

Testing

Audit Star tests use seprate configuration and db migration

Usage

Install pgmgr

    go get github.com/rnubel/pgmgr

Resetting DB and Run migration

      pgmgr db drop || true
      pgmgr db create
      pgmgr db migrate


Run tests

        go test ./...

Note

Since the purpose of audit_star is to audit a database, in order to test this
functionality, the provided tests create a test database by running the migration
contained within the ```db/migrate``` folder.  The migration was created and is
run using the [pgmgr Go migration tool](https://github.com/rnubel/pgmgr). So while
pgmgr is a dependency for the tests, it is not of dependency of audit_star itself.
*/
package main

/**/
