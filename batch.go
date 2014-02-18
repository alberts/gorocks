package gorocks

// #include "rocksdb/c.h"
import "C"

import (
	"io"
	"reflect"
	"unsafe"
)

// WriteBatch is a batching of Puts, and Deletes to be written atomically to a
// database. A WriteBatch is written when passed to DB.Write.
//
// To prevent memory leaks, call Close when the program no longer needs the
// WriteBatch object.
type WriteBatch struct {
	wbatch *C.rocksdb_writebatch_t
}

// NewWriteBatch creates a fully allocated WriteBatch.
func NewWriteBatch() *WriteBatch {
	wb := C.rocksdb_writebatch_create()
	return &WriteBatch{wb}
}

// Close releases the underlying memory of a WriteBatch.
func (w *WriteBatch) Close() {
	C.rocksdb_writebatch_destroy(w.wbatch)
}

// Put places a key-value pair into the WriteBatch for writing later.
//
// Both the key and value byte slices may be reused as WriteBatch takes a copy
// of them before returning.
//
func (w *WriteBatch) Put(key, value []byte) {
	// rocksdb_writebatch_put, and _delete call memcpy() (by way of
	// Memtable::Add) when called, so we do not need to worry about these
	// []byte being reclaimed by GC.
	var k, v *C.char
	if len(key) != 0 {
		k = (*C.char)(unsafe.Pointer(&key[0]))
	}
	if len(value) != 0 {
		v = (*C.char)(unsafe.Pointer(&value[0]))
	}

	lenk := len(key)
	lenv := len(value)

	C.rocksdb_writebatch_put(w.wbatch, k, C.size_t(lenk), v, C.size_t(lenv))
}

// Delete queues a deletion of the data at key to be deleted later.
//
// The key byte slice may be reused safely. Delete takes a copy of
// them before returning.
func (w *WriteBatch) Delete(key []byte) {
	C.rocksdb_writebatch_delete(w.wbatch,
		(*C.char)(unsafe.Pointer(&key[0])), C.size_t(len(key)))
}

// Clear removes all the enqueued Put and Deletes in the WriteBatch.
func (w *WriteBatch) Clear() {
	C.rocksdb_writebatch_clear(w.wbatch)
}

// Data returns a slice of the data in the batch. The data
// is not copied and the slice is only valid while the
// WriteBatch is open.
func (w *WriteBatch) Data() []byte {
	var size C.size_t
	p := C.rocksdb_writebatch_data(w.wbatch, &size)
	sliceHeader := &reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(p)),
		Len:  int(size),
		Cap:  int(size),
	}
	return *(*[]byte)(unsafe.Pointer(sliceHeader))
}

type WriteBatchIterator struct {
	data   []byte
	record Record
	err    error
}

type RecordType byte

const (
	RecordTypeDeletion RecordType = 0x0
	RecordTypeValue    RecordType = 0x1
	RecordTypeMerge    RecordType = 0x2
	RecordTypeLogData  RecordType = 0x3
)

type Record struct {
	Key   []byte
	Value []byte
	Type  RecordType
}

// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring
//    kTypeMerge varstring varstring
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

func (w *WriteBatch) NewIterator() *WriteBatchIterator {
	data := w.Data()
	if len(data) < 8+4 {
		return &WriteBatchIterator{}
	}
	return &WriteBatchIterator{data: data[12:]}
}

func (this *WriteBatchIterator) Next() bool {
	if this.err != nil || len(this.data) == 0 {
		return false
	}

	this.record.Key = nil
	this.record.Value = nil

	recordType := RecordType(this.data[0])
	this.record.Type = recordType
	this.data = this.data[1:]

	x, n := decodeVarint(this.data)
	if n == 0 {
		this.err = io.ErrShortBuffer
		return false
	}
	k := n + int(x)
	this.record.Key = this.data[n:k]
	this.data = this.data[k:]

	if recordType == RecordTypeValue || recordType == RecordTypeMerge {
		x, n := decodeVarint(this.data)
		if n == 0 {
			this.err = io.ErrShortBuffer
			return false
		}
		k := n + int(x)
		this.record.Value = this.data[n:k]
		this.data = this.data[k:]
	}

	return true
}

func (this *WriteBatchIterator) Record() *Record {
	return &this.record
}

func (this *WriteBatchIterator) Error() error {
	return this.err
}

func decodeVarint(buf []byte) (x uint64, n int) {
	// x, n already 0
	for shift := uint(0); shift < 64; shift += 7 {
		if n >= len(buf) {
			return 0, 0
		}
		b := uint64(buf[n])
		n++
		x |= (b & 0x7F) << shift
		if (b & 0x80) == 0 {
			return x, n
		}
	}
	// The number is too large to represent in a 64-bit value.
	return 0, 0
}
