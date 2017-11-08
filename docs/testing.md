[configuration]: https://github.com/go/audit_star/blob/master/audit/audit.yml
[db migration]: https://github.com/go/audit_star/blob/master/db/migrate/1496851823_audit_star.up.sql

Audit Star tests use seprate [configuration] and [db migration]

### Usage

Install pgmgr

`go get github.com/rnubel/pgmgr`

Resetting DB and Run migration

```
pgmgr db drop || true
pgmgr db create
pgmgr db migrate
```

Run tests

`go test ./...`

## Note:
Since the purpose of audit_star is to audit a database, in order to test this
functionality, the provided tests create a test database by running the migration
contained within the ```db/migrate``` folder.  The migration was created and is
run using the [pgmgr Go migration tool](https://github.com/rnubel/pgmgr). So while
pgmgr is a dependency for the tests, it is not of dependency of audit_star itself.
