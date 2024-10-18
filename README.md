s3db is a SQLite extension that stores tables in an S3-compatible object store.

What's New?
===========

v0.1.35 adds the `s3db_conn` table for per-connection `deadline` and `write_time` attributes.

v0.1.33 adds s3db_changes() and s3db_version()

Getting Started
===============

Check that your sqlite can load extensions. 
```
$ sqlite3
SQLite version 3.46.1 2024-08-13 09:16:08
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database.
sqlite> .dbconfig load_extension
     load_extension on
```
If you see `load_extension off`, build yourself 
[sqlite with default configuration](https://github.com/sqlite/sqlite).

```
sqlite> .open mydb.sqlite
sqlite> .load ./s3db
sqlite> create virtual table mytable using s3db (
```
Specify columns with constraints like you would a regular CREATE
TABLE.  Note that, as yet, s3db only uses the column names and
PRIMARY KEY; type affinity could be added later.
```
          columns='id PRIMARY KEY, name, email',
...
```
Specify the S3 parameters. Omit the `s3_endpoint` if using AWS, or
the `s3_prefix` if you don't plan to distinguish multiple tables
in the same bucket. Omit everything if you just want to fiddle with
a temporary bucket.
```
...
          s3_bucket='mybucket',
          s3_endpoint='https://my-s3-server-if-not-using-aws',
          s3_prefix='mydb',
...
```
We are willing to cache 1000 nodes in memory.
```
...
          node_cache_entries=1000);
```
Once created, tables remain part of the database and don't need to be recreated.
Of course, they can be mixed with regular tables.

Add some data:
```
sqlite> insert into mytable values (1,'jeff','old_address@example.org');
```

```
$ sqlite3 mydb.sqlite -cmd '.load ./s3db'
SQLite version 3.46.1 2024-08-13 09:16:08
Enter ".help" for usage hints.
sqlite> select * from mytable;
┌────┬──────┬─────────────────────────┐
│ id │ name │          email          │
├────┼──────┼─────────────────────────┤
│ 1  │ jeff │ old_address@example.org │
└────┴──────┴─────────────────────────┘
```

Tracking Changes
================

Continuing the example from above, use `s3db_changes` to see what new rows were
added between two versions:

```
sqlite> select s3db_version('mytable');
["1R2q4B_YS3HgUbGS9ycQWgP"]
sqlite> insert into mytable values (2,'joe','joe@example.org');
sqlite> update mytable set email='new_address@example.org' where id=1;
sqlite> select s3db_version('mytable');
["1R2q4B_N9x6HReYysstSo9I"]
sqlite> create virtual table additions using s3db_changes (table='mytable', from='["1R2q4B_YS3HgUbGS9ycQWgP"]', to='["1R2q4B_N9x6HReYysstSo9I"]');
sqlite> select * from additions;
┌────┬──────┬─────────────────────────┐
│ id │ name │          email          │
├────┼──────┼─────────────────────────┤
│ 1  │ jeff │ new_address@example.org │
│ 2  │ joe  │ joe@example.org         │
└────┴──────┴─────────────────────────┘
sqlite> drop table additions;
```
Flip the `from` and `to` to see what rows were removed between the two versions:
```
sqlite> create virtual table removals using s3db_changes (table='mytable', from='["1R2q4B_N9x6HReYysstSo9I"]', to='["1R2q4B_YS3HgUbGS9ycQWgP"]');
sqlite> select * from removals;
┌────┬──────┬─────────────────────────┐
│ id │ name │          email          │
├────┼──────┼─────────────────────────┤
│ 1  │ jeff │ old_address@example.org │
└────┴──────┴─────────────────────────┘
sqlite> drop table removals;
```

Performance
===========
Use transactions (BEGIN TRANSACTION, INSERT..., COMMIT) to include
multiple rows per table version.

Multiple Writers
================
Multiple writers can commit modifications from the same version.
The next reader will automatically merge both new versions. If there
are any conflicting rows, the winner is chosen by "last write wins",
on a per-column basis. Additionally, for any row, a DELETE supersedes
all UPDATEs with a later row modification time, until another INSERT
for the same key. In order to facilitate this, deletions consume
space until vacuumed.

Each writer can set its `write_time` corresponding to the ideal
time that the column values are to be affected, which can be useful
for idempotence. For example, `write_time` could rewind to the
original request time to avoid undoing later updates in retries.

Building from Source
====================
Requires Go >=1.22.
```
go vet ./...
go generate ./sqlite/sharedlib
go test ./...
```
produces the extension as `sqlite/sharedlib/s3db.so` (or
`sqlite/sharedlib/s3db.dylib` on MacOS).

Caveats
=======
* Each transaction may make a new version. Use the `s3db_vacuum()`
function to free up space from versions older than a certain time, e.g.:
```select * from s3db_vacuum('mytable', datetime('now','-7 days'))```
* Using `s3db` inside Go programs is best done using `github.com/jrhy/s3db/sqlite/mod`.
See [sqlite/example-go-app/](sqlite/example-go-app/) for an example. 

Function Reference
==================
`s3db_refresh(`*tablename*`)` reopens a table to show updates from
other writers.

`s3db_version(`*tablename*`)` returns a list of the versions merged to
form the current table.

Table-Valued Function Reference
===============================
`s3db_vacuum(`*tablename*`,`*before-timestamp*`)` removes versions older
than *before-timestamp*, e.g.
`select * from s3db_vacuum('mytable', datetime('now','-7 days'))`.

Virtual Table Reference
=======================
CREATE VIRTUAL TABLE *tablename* USING s3db() arguments:
* `columns='<colname> [primary key], ...',` columns and constraints
* (optional) `entries_per_node=<N>,`            the number of rows to store in per S3 object (defaults to 4096)
* (optional) `node_cache_entries=<N>,`          number of nodes to cache in memory (defaults to 0)
* (optional) `readonly,`                        don't write to S3
* (optional) `s3_bucket='mybucket',`            defaults to in-memory bucket
* (optional) `s3_endpoint='https://minio.example.com',` S3 endpoint, if not using AWS
* (optional) `s3_prefix='/prefix',`             separate tables within a bucket

CREATE VIRTUAL TABLE *tablename* USING s3db_changes() arguments:
* `table=`*tablename*, the s3db table to show changes of. Must be loaded already.
* `from=`*from-version*, the version to show changes since. Should be a previous result from `s3db_version()`.
* (optional) `to=`*to-version*, the version to show changes until. Defaults to the current version. 

`s3db_conn` sets per-connection attributes with the following columns:
* `deadline` - the timestamp after which network operations will be cancelled (defaults to forever), e.g.
`update s3db_conn set deadline=datetime('now','+3 seconds')`
* `write_time` - value modification timestamp, for resolving updates to the same column in a row with
"last-write wins" strategy idempotently

