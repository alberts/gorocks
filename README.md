# gorocks

gorocks is a Go wrapper for RocksDB.

It is based on levigo by Jeff Hodges.

The API has been godoc'ed and [is available on the
web](http://godoc.org/github.com/alberts/gorocks).

## Building

    CGO_CFLAGS="-I/path/to/rocksdb/include" CGO_LDFLAGS="-L/path/to/rocksdb" go get github.com/alberts/gorocks
