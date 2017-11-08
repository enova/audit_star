# Deployment
### HStore Extension

The auditing solution is built on top of the HStore data type in Postgres.  Hence, the HStore Extension must be installed in the database.

```sql
CREATE EXTENSION hstore with schema public;
```

Be sure that the extension is bubbled down to staging and development environments.  Otherwise the migrations builds/tests will fail.

### Run time parameters
The auditing solution uses PostgreSQL runtime parameters to pass metadata such as who made the change from the app to the audit system.  The parameters currently used now are `audit_star.changed_by` and `audit_star.changed_reason`. These are defaults that must be set at the database level, usually to an empty string.

```sql
ALTER DATABASE <your-db-name> SET audit_star.changed_by TO '';
ALTER DATABASE <your-db-name> SET audit_star.change_reason TO '';
```

Be sure that the setting is bubbled down to staging and development environments.  Otherwise the migrations builds/tests will fail.

Before using audit_star, database-specific configuration must be made to the ```audit.yml```
file.  By default, audit_star will look in the current directory from which it is
being executed, but the optional parameter ```-cfg``` allows the user to provide
an alternative path to the ```audit.yml``` file.

The database-specific configuration along with accepted values are detailed in the
example file provided in `audit.yml` (copied below).

```yaml
# database config information
# host: database host name
# post: database port number; postgres default is 5432
# db_name: database_name (name of the db to audit)
# username: database username used to connect
# password: databsase password used to connect

# audit star config information
# excluded_tables:
#   - exclude this_schema.this_table
# excluded_schemas:
#   - exclude_this_schema
# owner: app__owner (only audit tables owned by this user, if not specified will audit *every* table it can)
# log_client_query: false (toggle logging of query that caused the change)
# security: definer/invoker (security level of audit function - usually definer on release to avoid race conditions with defining permissions)
```

## Installation
The preferred way to deploy Go binaries is using `go get` and `go install`.
Optionally, you can clone the repo manually into your $GOPATH and run `go build`.

Since audit_star is written in [Go](https://golang.org/), in order to build audit_star you must have Go
installed on your machine.  Alternatively, you can build the binary on one machine
and then copy the binary to a machine, as needed, before invoking it, assuming the
machines share the same architecture.  Go also supports [cross-compilation](https://dave.cheney.net/2012/09/08/an-introduction-to-cross-compilation-with-go)
which allows you to build the binary on a machine with one architecture and run
it on a machine with a completely different architecture.

### Using `go get` (preferred)
You can download audit_star by running `go get github.com/enova/audit_star`.
This will download audit_star into your $GOPATH and install the binary into your $GOPATH/bin folder.
If your $GOPATH/bin folder is in your $PATH then you can run audit_star from anywhere by executing `audit_star`.

### Building with ```go build```
After cloning the git repo, ```cd``` into the directory and run ```go build```.  Assuming your Go is set up properly, this will create an ```audit_star``` binary in the current directly.  You can then execute this binary by executing ```./audit_star```.
