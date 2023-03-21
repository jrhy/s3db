s3db is a SQLite extension that stores tables in an S3-compatible object store.

Getting Started
===============

Check that your sqlite can load extensions. 
```
$ sqlite3
SQLite version 3.41.0 2023-02-21 18:09:37
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
Specify columns with constraints like you would a regular CREATE TABLE.
Note that, as yet, s3db only uses the column names and PRIMARY KEY;
type affinity could be added later.
```
          columns='id PRIMARY KEY, name, email',
...
```
Specify the S3 parameters. You can omit the `s3_endpoint` if using AWS,
or the `s3_prefix` if you don't plan to distinguish multiple tables in
the same bucket.
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

Now we can add some data:
```
sqlite> insert into mytable values (1,'jeff','********@rhyason.org');
```

```
$ sqlite3 mydb.sqlite -cmd '.load ./s3db'
SQLite version 3.41.0 2023-02-21 18:09:37
Enter ".help" for usage hints.
sqlite> select * from mytable;
┌────┬──────┬──────────────────────┐
│ id │ name │        email         │
├────┼──────┼──────────────────────┤
│ 1  │ jeff │ ********@rhyason.org │
└────┴──────┴──────────────────────┘
```

CREATE VIRTUAL TABLE Options
============================
```
 columns='<colname> [type] [primary key] [not null], ...',
                                   columns and constraints
 deadline='<N>[s,m,h,d]',          timeout operations if they take too long
 entries_per_node=<N>,             the number of rows to store in per S3 object
 node_cache_entries=<N>,           number of nodes to cache in memory
[s3_bucket='mybucket',]            defaults to in-memory bucket
[s3_endpoint='https://minio.example.com',]
                                   optional S3 endpoint (if not using AWS)
[s3_prefix='/prefix',]             separate tables within a bucket
[write_time='2006-01-02 15:04:05',]
                                   value modification time, for idempotence, from request time
```

Multiple Writers
================
Multiple writers can commit modifications from the same version.
The next reader will automatically merge both new versions. If
there are any conflicting rows, the winner is chosen by "last
write wins", on a per-column basis. Additionally, for any row, a
DELETE supersedes all UPDATEs with a later row modification time,
until another INSERT for the same key. In order to facilitate this,
deletions consume space until vacuumed.

Each writer can set its `write_time` corresponding to the ideal
time that the column values are to be affected, which can be useful
for idempotence.

Building from Source
====================
Requires Go 1.19. 
```
go vet -tags sqlite_vtable ./...
go test -tags sqlite_vtable ./...
go generate ./sqlite
```
produces the extension as `sqlite/s3db.so` (or `sqlite/s3db.dylib` on MacOS). 

Caveats
=======
* Each transaction may make a new version. Use the `s3db_vacuum()`
function to free up space from versions older than a certain time, e.g.:
```select * from s3db_vacuum('mytable', datetime('now','-7 days'))```
* Using the extension inside Go programs requires the program be built with the Go build option `-linkshared`.
Therefore this extension cannot be used in Go programs on MacOS due to lack of 
support ( `-linkshared not supported on darwin/arm64`). A version that uses 
[go-sqlite3](https://github.com/mattn/go-sqlite3) is possible--please
signal any interest in an issue.

