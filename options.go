package gorocks

// #include "rocksdb/c.h"
import "C"

import (
	"time"
)

// CompressionOpt is a value for Options.SetCompression.
type CompressionOpt int

// Known compression arguments for Options.SetCompression.
const (
	NoCompression     = CompressionOpt(0)
	SnappyCompression = CompressionOpt(1)
)

type CompactionStyle int

const (
	LevelStyleCompaction     = CompactionStyle(0)
	UniversalStyleCompaction = CompactionStyle(1)
)

// Options represent all of the available options when opening a database with
// Open. Options should be created with NewOptions.
//
// It is usually with to call SetCache with a cache object. Otherwise, all
// data will be read off disk.
//
// To prevent memory leaks, Close must be called on an Options when the
// program no longer needs it.
type Options struct {
	Opt *C.rocksdb_options_t
}

// ReadOptions represent all of the available options when reading from a
// database.
//
// To prevent memory leaks, Close must called on a ReadOptions when the
// program no longer needs it.
type ReadOptions struct {
	Opt *C.rocksdb_readoptions_t
}

// WriteOptions represent all of the available options when writeing from a
// database.
//
// To prevent memory leaks, Close must called on a WriteOptions when the
// program no longer needs it.
type WriteOptions struct {
	Opt *C.rocksdb_writeoptions_t
}

// NewOptions allocates a new Options object.
func NewOptions() *Options {
	opt := C.rocksdb_options_create()
	return &Options{opt}
}

// NewReadOptions allocates a new ReadOptions object.
func NewReadOptions() *ReadOptions {
	opt := C.rocksdb_readoptions_create()
	return &ReadOptions{opt}
}

// NewWriteOptions allocates a new WriteOptions object.
func NewWriteOptions() *WriteOptions {
	opt := C.rocksdb_writeoptions_create()
	return &WriteOptions{opt}
}

// Close deallocates the Options, freeing its underlying C struct.
func (o *Options) Close() {
	C.rocksdb_options_destroy(o.Opt)
}

// SetComparator sets the comparator to be used for all read and write
// operations.
//
// The comparator that created a database must be the same one (technically,
// one with the same name string) that is used to perform read and write
// operations.
//
// The default comparator is usually sufficient.
func (o *Options) SetComparator(cmp *C.rocksdb_comparator_t) {
	C.rocksdb_options_set_comparator(o.Opt, cmp)
}

// SetErrorIfExists, if passed true, will cause the opening of a database that
// already exists to throw an error.
func (o *Options) SetErrorIfExists(error_if_exists bool) {
	eie := boolToUchar(error_if_exists)
	C.rocksdb_options_set_error_if_exists(o.Opt, eie)
}

// SetCache places a cache object in the database when a database is opened.
//
// This is usually wise to use. See also ReadOptions.SetFillCache.
func (o *Options) SetCache(cache *Cache) {
	C.rocksdb_options_set_cache(o.Opt, cache.Cache)
}

// SetEnv sets the Env object for the new database handle.
func (o *Options) SetEnv(env *Env) {
	C.rocksdb_options_set_env(o.Opt, env.Env)
}

// SetInfoLog sets a *C.rocksdb_logger_t object as the informational logger
// for the database.
func (o *Options) SetInfoLog(log *C.rocksdb_logger_t) {
	C.rocksdb_options_set_info_log(o.Opt, log)
}

// SetWriteBufferSize sets the number of bytes the database will build up in
// memory (backed by an unsorted log on disk) before converting to a sorted
// on-disk file.
func (o *Options) SetWriteBufferSize(s int) {
	C.rocksdb_options_set_write_buffer_size(o.Opt, C.size_t(s))
}

func (o *Options) SetMaxWriteBuffers(s int) {
	C.rocksdb_options_set_max_write_buffer_number(o.Opt, C.int(s))
}

func (o *Options) SetMinWriteBuffersToMerge(s int) {
	C.rocksdb_options_set_min_write_buffer_number_to_merge(o.Opt, C.int(s))
}

// SetParanoidChecks, when called with true, will cause the database to do
// aggressive checking of the data it is processing and will stop early if it
// detects errors.
//
// See the LevelDB documentation docs for details.
func (o *Options) SetParanoidChecks(pc bool) {
	C.rocksdb_options_set_paranoid_checks(o.Opt, boolToUchar(pc))
}

// SetMaxOpenFiles sets the number of files than can be used at once by the
// database.
//
// See the LevelDB documentation for details.
func (o *Options) SetMaxOpenFiles(n int) {
	C.rocksdb_options_set_max_open_files(o.Opt, C.int(n))
}

// SetBlockSize sets the approximate size of user data packed per block.
//
// The default is roughly 4096 uncompressed bytes. A better setting depends on
// your use case. See the LevelDB documentation for details.
func (o *Options) SetBlockSize(s int) {
	C.rocksdb_options_set_block_size(o.Opt, C.size_t(s))
}

// SetBlockRestartInterval is the number of keys between restarts points for
// delta encoding keys.
//
// Most clients should leave this parameter alone. See the LevelDB
// documentation for details.
func (o *Options) SetBlockRestartInterval(n int) {
	C.rocksdb_options_set_block_restart_interval(o.Opt, C.int(n))
}

// SetCompression sets whether to compress blocks using the specified
// compresssion algorithm.
//
// The default value is SnappyCompression and it is fast enough that it is
// unlikely you want to turn it off. The other option is NoCompression.
//
// If the LevelDB library was built without Snappy compression enabled, the
// SnappyCompression setting will be ignored.
func (o *Options) SetCompression(t CompressionOpt) {
	C.rocksdb_options_set_compression(o.Opt, C.int(t))
}

// SetCreateIfMissing causes Open to create a new database on disk if it does
// not already exist.
func (o *Options) SetCreateIfMissing(b bool) {
	C.rocksdb_options_set_create_if_missing(o.Opt, boolToUchar(b))
}

// SetFilterPolicy causes Open to create a new database that will uses filter
// created from the filter policy passed in.
func (o *Options) SetFilterPolicy(fp *FilterPolicy) {
	var policy *C.rocksdb_filterpolicy_t
	if fp != nil {
		policy = fp.Policy
	}
	C.rocksdb_options_set_filter_policy(o.Opt, policy)
}

// SetMaxBackgroundCompactions sets the maximum number of concurrent
// background jobs, submitted to the default LOW priority thread pool
func (o *Options) SetMaxBackgroundCompactions(n int) {
	C.rocksdb_options_set_max_background_compactions(o.Opt, C.int(n))
}

// SetMaxBackgroundFlushes sets the maximum number of concurrent
// background memtable flush jobs, submitted to the HIGH priority
// thread pool. By default, all background jobs (major compaction
// and memtable flush) go to the LOW priority pool. If this option
// is set to a positive number, memtable flush jobs will be submitted
// to the HIGH priority pool. It is important when the same Env is
// shared by multiple db instances. Without a separate pool, long
// running major compaction jobs could potentially block memtable
// flush jobs of other db instances, leading to unnecessary Put stalls.
func (o *Options) SetMaxBackgroundFlushes(n int) {
	C.rocksdb_options_set_max_background_flushes(o.Opt, C.int(n))
}

// SetMemtableVectorRep causes MemTableReps that are backed by a
// std::vector to be used. On iteration, the vector is sorted. This
// is useful for workloads where iteration is very rare and writes
// are generally not issued after reads begin.
func (o *Options) SetMemtableVectorRep() {
	C.rocksdb_options_set_memtable_vector_rep(o.Opt)
}

func (o *Options) SetStatsDumpPeriod(period time.Duration) {
	periodSec := C.uint(period.Seconds())
	C.rocksdb_options_set_stats_dump_period_sec(o.Opt, periodSec)
}

func (o *Options) SetNumLevels(levels int) {
	C.rocksdb_options_set_num_levels(o.Opt, C.int(levels))
}

func (o *Options) SetLevel0FileNumCompactionTrigger(n int) {
	C.rocksdb_options_set_level0_file_num_compaction_trigger(o.Opt, C.int(n))
}

func (o *Options) SetLevel0SlowdownWritesTrigger(n int) {
	C.rocksdb_options_set_level0_slowdown_writes_trigger(o.Opt, C.int(n))
}

func (o *Options) SetLevel0StopWritesTrigger(n int) {
	C.rocksdb_options_set_level0_stop_writes_trigger(o.Opt, C.int(n))
}

func (o *Options) SetTargetFileSizeBase(n uint64) {
	C.rocksdb_options_set_target_file_size_base(o.Opt, C.uint64_t(n))
}

func (o *Options) SetDisableSeekCompaction(b bool) {
	C.rocksdb_options_set_disable_seek_compaction(o.Opt, boolToInt(b))
}

func (o *Options) SetMaxBytesForLevelBase(n uint64) {
	C.rocksdb_options_set_max_bytes_for_level_base(o.Opt, C.uint64_t(n))
}

func (o *Options) EnableStatistics() {
	C.rocksdb_options_enable_statistics(o.Opt)
}

func (o *Options) SetCompactionStyle(style CompactionStyle) {
	C.rocksdb_options_set_compaction_style(o.Opt, C.int(style))
	// TODO this will leak if Options is discarded
	uco := C.rocksdb_universal_compaction_options_create()

	//C.rocksdb_universal_compaction_options_set_size_ratio(uco, ratio)

	//C.rocksdb_universal_compaction_options_set_min_merge_width(uco, w)

	C.rocksdb_universal_compaction_options_set_max_merge_width(uco, 16)

	// Trigger compaction if size amplification exceeds 110%
	C.rocksdb_universal_compaction_options_set_max_size_amplification_percent(uco, 110)

	// TODO tune this so that we don't compress initially
	//C.rocksdb_universal_compaction_options_set_compression_size_percent(uco, p)

	//C.rocksdb_universal_compaction_options_set_stop_style(uco, style)

	C.rocksdb_options_set_universal_compaction_options(o.Opt, uco)
}

func (o *Options) SetMinLevelToCompress(level int) {
	C.rocksdb_options_set_min_level_to_compress(o.Opt, C.int(level))
}

// Close deallocates the ReadOptions, freeing its underlying C struct.
func (ro *ReadOptions) Close() {
	C.rocksdb_readoptions_destroy(ro.Opt)
}

// SetVerifyChecksums controls whether all data read with this ReadOptions
// will be verified against corresponding checksums.
//
// It defaults to false. See the LevelDB documentation for details.
func (ro *ReadOptions) SetVerifyChecksums(b bool) {
	C.rocksdb_readoptions_set_verify_checksums(ro.Opt, boolToUchar(b))
}

// SetFillCache controls whether reads performed with this ReadOptions will
// fill the Cache of the server. It defaults to true.
//
// It is useful to turn this off on ReadOptions for DB.Iterator (and DB.Get)
// calls used in offline threads to prevent bulk scans from flushing out live
// user data in the cache.
//
// See also Options.SetCache
func (ro *ReadOptions) SetFillCache(b bool) {
	C.rocksdb_readoptions_set_fill_cache(ro.Opt, boolToUchar(b))
}

// SetSnapshot causes reads to provided as they were when the passed in
// Snapshot was created by DB.NewSnapshot. This is useful for getting
// consistent reads during a bulk operation.
//
// See the LevelDB documentation for details.
func (ro *ReadOptions) SetSnapshot(snap *Snapshot) {
	var s *C.rocksdb_snapshot_t
	if snap != nil {
		s = snap.snap
	}
	C.rocksdb_readoptions_set_snapshot(ro.Opt, s)
}

// Close deallocates the WriteOptions, freeing its underlying C struct.
func (wo *WriteOptions) Close() {
	C.rocksdb_writeoptions_destroy(wo.Opt)
}

// SetSync controls whether each write performed with this WriteOptions will
// be flushed from the operating system buffer cache before the write is
// considered complete.
//
// If called with true, this will signficantly slow down writes. If called
// with false, and the host machine crashes, some recent writes may be
// lost. The default is false.
//
// See the LevelDB documentation for details.
func (wo *WriteOptions) SetSync(b bool) {
	C.rocksdb_writeoptions_set_sync(wo.Opt, boolToUchar(b))
}

// DisableWAL completely disables the Write Ahead Log. Writes will not go
// to the log at all and may be lost in an event of process crash.
func (wo *WriteOptions) DisableWAL(b bool) {
	C.rocksdb_writeoptions_disable_WAL(wo.Opt, boolToInt(b))
}
