# s3db: Serverless database and diffing for S3
[![GoDoc](https://godoc.org/github.com/jrhy/s3db?status.svg)](https://godoc.org/github.com/jrhy/s3db)

s3db turns your S3 bucket into a database. You can also learn what changed
since the last time you looked.  Now your database is also your event stream!
🤯 

# Details
s3db is an opinionated modern serverless datastore built on diffing
that integrates streaming, reduces dependencies and makes it natural
to write distributed systems with fewer idempotency bugs.

s3db makes it easy to put and get structured data in S3 from Go, AND 
track changes.
s3db doesn't depend on any services other than S3-compatible storage.

s3db is on a mission to make it easier to build applications that can
take advantage of the high availability and durability of object
storage, while also being predictable and resilient in the face of failures,
by leveraging immutable data and CRDTs to get the best from local-first
and shared-nothing architectures.

# Requirements
- go1.14+
- S3-compatible storage, like minio, Wasabi, or AWS

# Getting started
Check out the [Getting Started Guide](GETTINGSTARTED.md).

# Unrelated
* [Simple Sloppy Semantic Database](https://en.wikipedia.org/wiki/Simple_Sloppy_Semantic_Database) at s3db.org predates Amazon S3

# Links
* Go package documentation [godoc.org/github.com/jrhy/s3db](https://godoc.org/github.com/jrhy/s3db)

