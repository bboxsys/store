// Package store is a JSON document store on top of BadgerDB.
//
// Keys are strings of the form "prefix:id"; values are the JSON encoding of
// the stored object. Account-scoped helpers prepend an account ID to build
// "{accountID}:{prefix}:{key}" for multi-tenant data.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"
)

// Logger is the minimal logging surface the store needs. Any logger with
// these four methods can be passed in Options; when none is given the store
// builds a zerolog console logger.
type Logger interface {
	Debug(msg string)
	Info(msg string)
	Warn(msg string)
	Error(msg string)
}

// Store wraps a BadgerDB instance.
type Store struct {
	DB        *badger.DB
	Logger    Logger
	Location  string
	putOps    atomic.Int64
	getOps    atomic.Int64
	deleteOps atomic.Int64
}

// Options configures a Store. Application and Component are only used to
// tag the default logger when Logger is nil.
type Options struct {
	Location       string `json:"location"`
	Application    string `json:"application"`
	Component      string `json:"component"`
	IndexCacheSize int64  `json:"index_cache_size"`
	BlockCacheSize int64  `json:"block_cache_size"`
	Logger         Logger `json:"-"`
}

// BadgerLogger adapts a Logger to badger's Logger interface.
type BadgerLogger struct {
	logger Logger
}

// NewBadgerLogger creates a new badger-compatible logger.
func NewBadgerLogger(l Logger) *BadgerLogger {
	return &BadgerLogger{logger: l}
}

// Errorf implements badger.Logger.
func (bl *BadgerLogger) Errorf(format string, args ...interface{}) {
	bl.logger.Error(fmt.Sprintf(format, args...))
}

// Warningf implements badger.Logger.
func (bl *BadgerLogger) Warningf(format string, args ...interface{}) {
	bl.logger.Warn(fmt.Sprintf(format, args...))
}

// Infof implements badger.Logger.
func (bl *BadgerLogger) Infof(format string, args ...interface{}) {
	bl.logger.Info(fmt.Sprintf(format, args...))
}

// Debugf implements badger.Logger.
func (bl *BadgerLogger) Debugf(format string, args ...interface{}) {
	bl.logger.Debug(fmt.Sprintf(format, args...))
}

// zerologAdapter satisfies Logger with a zerolog.Logger.
type zerologAdapter struct {
	l zerolog.Logger
}

func (z zerologAdapter) Debug(msg string) { z.l.Debug().Msg(msg) }
func (z zerologAdapter) Info(msg string)  { z.l.Info().Msg(msg) }
func (z zerologAdapter) Warn(msg string)  { z.l.Warn().Msg(msg) }
func (z zerologAdapter) Error(msg string) { z.l.Error().Msg(msg) }

// NewZerologLogger wraps a zerolog.Logger as a store Logger.
func NewZerologLogger(l zerolog.Logger) Logger {
	return zerologAdapter{l: l}
}

func defaultLogger(application, component string) Logger {
	l := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
		Level(zerolog.InfoLevel).
		With().Timestamp().
		Str("application", application).
		Str("component", component).
		Logger()
	return zerologAdapter{l: l}
}

// NewStore opens (creating if needed) the BadgerDB at options.Location.
func NewStore(options *Options) (*Store, error) {
	store := Store{}
	store.Logger = options.Logger
	if store.Logger == nil {
		store.Logger = defaultLogger(options.Application, options.Component)
	}
	opts := badger.DefaultOptions(options.Location)
	opts.Dir = options.Location
	opts.ValueDir = opts.Dir
	opts.Logger = NewBadgerLogger(store.Logger)
	if options.IndexCacheSize > 0 {
		opts.IndexCacheSize = options.IndexCacheSize
	}
	if options.BlockCacheSize > 0 {
		opts.BlockCacheSize = options.BlockCacheSize
	}
	db, err := badger.Open(opts)
	if err != nil {
		return &store, err
	}
	store.DB = db
	store.Location = options.Location
	return &store, nil
}

// Close closes the underlying database.
func (store *Store) Close() error {
	return store.DB.Close()
}

// GetKeyExists reports whether the raw value stored at key is "true". A missing
// key reports false with no error; any other error (I/O failure, closed DB) is
// returned so callers can distinguish "absent" from "could not read".
func (store *Store) GetKeyExists(key string) (bool, error) {
	i, err := store.GetOneRaw(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return i == "true", nil
}

// PutOne stores obj as JSON under key.
func (store *Store) PutOne(key string, obj interface{}) error {
	store.putOps.Add(1)
	return store.DB.Update(func(txn *badger.Txn) error {
		value, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		return txn.Set([]byte(key), value)
	})
}

// PutOneWithTTL stores obj as JSON under key with an expiry.
func (store *Store) PutOneWithTTL(key string, obj interface{}, ttl time.Duration) error {
	store.putOps.Add(1)
	return store.DB.Update(func(txn *badger.Txn) error {
		value, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		e := badger.NewEntry([]byte(key), value).WithTTL(ttl)
		return txn.SetEntry(e)
	})
}

// PutOneWithTTLAndMeta stores obj with an expiry and a user meta byte.
func (store *Store) PutOneWithTTLAndMeta(key string, obj interface{}, ttl time.Duration, meta byte) error {
	store.putOps.Add(1)
	return store.DB.Update(func(txn *badger.Txn) error {
		value, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		e := badger.NewEntry([]byte(key), value).WithMeta(meta).WithTTL(ttl)
		return txn.SetEntry(e)
	})
}

// PutOneRaw stores a string value verbatim.
func (store *Store) PutOneRaw(key string, obj string) error {
	store.putOps.Add(1)
	return store.DB.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), []byte(obj))
	})
}

// PutOneRawWithTTL stores a string value verbatim with an expiry.
func (store *Store) PutOneRawWithTTL(key string, obj string, ttl time.Duration) error {
	store.putOps.Add(1)
	return store.DB.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry([]byte(key), []byte(obj)).WithTTL(ttl)
		return txn.SetEntry(e)
	})
}

// PutOneRawWithTTLAndMeta stores a string value verbatim with an expiry and meta byte.
func (store *Store) PutOneRawWithTTLAndMeta(key string, obj string, ttl time.Duration, meta byte) error {
	store.putOps.Add(1)
	return store.DB.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry([]byte(key), []byte(obj)).WithMeta(meta).WithTTL(ttl)
		return txn.SetEntry(e)
	})
}

// PutSlice stores every element of objs under "prefix:<id>", where id is the
// gjson path of the element's identifier field.
func (store *Store) PutSlice(prefix string, id string, objs interface{}) error {
	store.putOps.Add(1)
	wb := store.DB.NewWriteBatch()
	defer wb.Cancel()
	switch reflect.TypeOf(objs).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(objs)
		for i := 0; i < s.Len(); i++ {
			obj := s.Index(i).Interface()
			value, err := json.Marshal(obj)
			if err != nil {
				return err
			}
			result := gjson.GetBytes(value, id)
			// Without a value at the id path every such element would be stored
			// under the same "prefix:" key and silently overwrite the others.
			if !result.Exists() || result.String() == "" {
				return fmt.Errorf("store: element %d has no value at id path %q", i, id)
			}
			err = wb.Set([]byte(fmt.Sprintf("%s:%s", prefix, result.String())), value)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("did not detect the type")
	}
	return wb.Flush()
}

// PutJSONPipe reads a JSON array of objects from stream and stores each under
// "prefix:<uuid>". Objects without a uuid field are skipped and logged.
func (store *Store) PutJSONPipe(prefix string, stream *io.PipeReader) error {
	return store.PutStream(prefix, stream)
}

// PutStream reads a JSON array of objects from r and stores each under
// "prefix:<uuid>". The reader is typically an http.Response.Body. A malformed
// stream returns an error; individual objects that fail to store are logged
// and skipped.
func (store *Store) PutStream(prefix string, r io.Reader) error {
	decoder := json.NewDecoder(r)
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("reading opening token %v: %w", token, err)
	}
	// A single WriteBatch amortizes commits across all objects instead of
	// opening one transaction per object, which is far faster for bulk ingest.
	wb := store.DB.NewWriteBatch()
	defer wb.Cancel()
	for decoder.More() {
		var obj interface{}
		err := decoder.Decode(&obj)
		if err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("decoding stream object: %w", err)
		}
		objMap, ok := obj.(map[string]interface{})
		if !ok {
			store.Logger.Error("PutStream: object is not a JSON object; skipping")
			continue
		}
		uuid, ok := objMap["uuid"]
		if !ok {
			store.Logger.Error("PutStream: object has no uuid field; skipping")
			continue
		}
		value, err := json.Marshal(obj)
		if err != nil {
			store.Logger.Error(fmt.Sprintf("PutStream: error marshaling object: %v", err))
			continue
		}
		if err := wb.Set([]byte(fmt.Sprintf("%s:%s", prefix, uuid)), value); err != nil {
			return fmt.Errorf("PutStream: error writing object: %w", err)
		}
	}
	token, err = decoder.Token()
	if err != nil {
		return fmt.Errorf("reading closing token %v: %w", token, err)
	}
	return wb.Flush()
}

// Filter matches documents whose gjson Path evaluates to Value.
type Filter struct {
	Path  string `json:"path"`
	Value string `json:"value"`
}

// TimeRange bounds a query by the start/end timestamps encoded in keys.
type TimeRange struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// Query describes a prefix scan with optional filters and paging.
type Query struct {
	Prefix    string    `json:"prefix"`
	Filters   []*Filter `json:"filters"`
	Limit     int       `json:"limit"`
	Offset    int       `json:"offset"`
	TimeRange TimeRange `json:"time_range"`
}

// sliceAppender decodes JSON values one at a time and appends them to a
// caller-supplied pointer-to-slice. It avoids concatenating the entire result
// set into a single JSON buffer (roughly halving peak memory for large scans)
// and decodes each value independently, so one malformed or non-JSON value is
// reported precisely rather than corrupting the whole batch.
type sliceAppender struct {
	slice    reflect.Value
	elemType reflect.Type
}

func newSliceAppender(v interface{}) (*sliceAppender, error) {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() || rv.Elem().Kind() != reflect.Slice {
		return nil, errors.New("store: destination must be a non-nil pointer to a slice")
	}
	slice := rv.Elem()
	return &sliceAppender{slice: slice, elemType: slice.Type().Elem()}, nil
}

func (a *sliceAppender) append(raw []byte) error {
	elemPtr := reflect.New(a.elemType)
	if err := json.Unmarshal(raw, elemPtr.Interface()); err != nil {
		return err
	}
	a.slice.Set(reflect.Append(a.slice, elemPtr.Elem()))
	return nil
}

// Get unmarshals every value under prefix into v, which must be a pointer to
// a slice.
func (store *Store) Get(prefix string, v interface{}) error {
	appender, err := newSliceAppender(v)
	if err != nil {
		return err
	}
	return store.DB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		p := []byte(prefix)
		for it.Seek(p); it.ValidForPrefix(p); it.Next() {
			val, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if err := appender.append(val); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetWithQuery unmarshals every value under query.Prefix that matches all of
// query.Filters (and the time range, if set) into v, honoring query.Offset and
// query.Limit for pagination.
func (store *Store) GetWithQuery(query *Query, v interface{}) error {
	appender, err := newSliceAppender(v)
	if err != nil {
		return err
	}
	return store.DB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		p := []byte(query.Prefix)
		matched := 0
		skipped := 0
		for it.Seek(p); it.ValidForPrefix(p); it.Next() {
			item := it.Item()
			if (query.TimeRange != TimeRange{}) && !inTimeRange(string(item.Key()), query.TimeRange) {
				continue
			}
			// Peek the value to evaluate filters without copying rejected values.
			var matches bool
			if err := item.Value(func(val []byte) error {
				matches = matchesFilters(val, query.Filters)
				return nil
			}); err != nil {
				return err
			}
			if !matches {
				continue
			}
			if query.Offset > 0 && skipped < query.Offset {
				skipped++
				continue
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if err := appender.append(val); err != nil {
				return err
			}
			matched++
			if query.Limit > 0 && matched >= query.Limit {
				break
			}
		}
		return nil
	})
}

// GetByMeta unmarshals every value whose user meta byte equals meta into v.
func (store *Store) GetByMeta(meta byte, v interface{}) error {
	return store.DB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		objs := make([]byte, 0)
		num := 0
		for it.Rewind(); it.Valid(); it.Next() {
			if it.Item().UserMeta() == meta {
				if num == 0 {
					objs = append(objs, []byte("[")...)
				}
				num++
				item := it.Item()
				val, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				objs = append(objs, val...)
				objs = append(objs, []byte(",")...)
			}
		}
		if num != 0 {
			objs = objs[:len(objs)-len([]byte(","))]
			objs = append(objs, []byte("]")...)
			return json.Unmarshal(objs, v)
		}
		return nil
	})
}

// GetStream writes a JSON array of matching values to w on a goroutine that
// is tracked by wg. Query.Prefix is scanned as "prefix:".
func (store *Store) GetStream(w io.Writer, wg *sync.WaitGroup, query *Query) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := store.DB.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			prefix := []byte(fmt.Sprintf("%s:", query.Prefix))
			written := 0
			skipped := 0
			write := func(b []byte) error {
				_, err := w.Write(b)
				return err
			}
			if err := write([]byte("[")); err != nil {
				return err
			}
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				val, err := item.ValueCopy(nil)
				if err != nil {
					// The array is already open; close it so the client at
					// least receives valid JSON, but surface the error to the log.
					_ = write([]byte("]"))
					return err
				}
				if !matchesFilters(val, query.Filters) {
					continue
				}
				if query.Offset > 0 && skipped < query.Offset {
					skipped++
					continue
				}
				if written != 0 {
					if err := write([]byte(",")); err != nil {
						return err
					}
				}
				if err := write(val); err != nil {
					return err
				}
				written++
				if query.Limit > 0 && written >= query.Limit {
					break
				}
			}
			return write([]byte("]"))
		})
		if err != nil {
			store.Logger.Error(fmt.Sprintf("GetStream error: %v", err))
		}
	}()
}

// GetStreamProjected streams a JSON array of transformed values under
// "query.Prefix:" to w on a goroutine tracked by wg, mirroring GetStream but
// applying transform to each stored value before writing. transform returns the
// bytes to emit and whether to include the item (false skips it) -- typically a
// projection to a few fields (for example map markers: id/lat/lon) so the wire
// payload is a fraction of the full documents. Emitted bytes must be valid
// JSON. Filters, Offset, and Limit are honored as in GetStream.
//
// This uses a single iterator deliberately: benchmarks showed Badger's parallel
// Stream framework is slower and allocates ~19x more memory for prefix scans of
// our size (it targets full-DB backup across many SSTables), so the lean
// single-iterator scan wins here.
func (store *Store) GetStreamProjected(w io.Writer, wg *sync.WaitGroup, query *Query, transform func(value []byte) ([]byte, bool)) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := store.DB.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			prefix := []byte(fmt.Sprintf("%s:", query.Prefix))
			written := 0
			skipped := 0
			write := func(b []byte) error {
				_, err := w.Write(b)
				return err
			}
			if err := write([]byte("[")); err != nil {
				return err
			}
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				val, err := it.Item().ValueCopy(nil)
				if err != nil {
					_ = write([]byte("]"))
					return err
				}
				if !matchesFilters(val, query.Filters) {
					continue
				}
				out, include := val, true
				if transform != nil {
					out, include = transform(val)
				}
				if !include {
					continue
				}
				if query.Offset > 0 && skipped < query.Offset {
					skipped++
					continue
				}
				if written != 0 {
					if err := write([]byte(",")); err != nil {
						return err
					}
				}
				if err := write(out); err != nil {
					return err
				}
				written++
				if query.Limit > 0 && written >= query.Limit {
					break
				}
			}
			return write([]byte("]"))
		})
		if err != nil {
			store.Logger.Error(fmt.Sprintf("GetStreamProjected error: %v", err))
		}
	}()
}

// GetScopedStreamProjected streams an account-scoped prefix with a projection.
// See GetStreamProjected.
func (store *Store) GetScopedStreamProjected(w io.Writer, wg *sync.WaitGroup, accountID string, query *Query, transform func(value []byte) ([]byte, bool)) {
	store.GetStreamProjected(w, wg, scopedQueryNoTrailingColon(accountID, query), transform)
}

// GetCount returns the number of values under "prefix:" matching the filters.
func (store *Store) GetCount(query *Query) (int, error) {
	count := 0
	err := store.DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = len(query.Filters) > 0
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte(fmt.Sprintf("%s:", query.Prefix))
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if len(query.Filters) == 0 {
				count++
				continue
			}
			val, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if matchesFilters(val, query.Filters) {
				count++
			}
		}
		return nil
	})
	return count, err
}

// GetOne unmarshals the value at key into obj.
func (store *Store) GetOne(key string, obj interface{}) error {
	store.getOps.Add(1)
	return store.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return json.Unmarshal(val, obj)
	})
}

// GetOneRaw returns the value at key as a string.
func (store *Store) GetOneRaw(key string) (string, error) {
	store.getOps.Add(1)
	var result string
	err := store.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		result = string(val)
		return nil
	})
	if err != nil {
		return "", err
	}
	return result, nil
}

// GetOneByFilter unmarshals the first value under "prefix:" matching all
// filters into obj. obj is left untouched when nothing matches.
func (store *Store) GetOneByFilter(prefix string, filters *[]Filter, obj interface{}) error {
	return store.DB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		p := []byte(fmt.Sprintf("%s:", prefix))
		for it.Seek(p); it.ValidForPrefix(p); it.Next() {
			val, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			matches := 0
			for _, filter := range *filters {
				if gjson.GetBytes(val, filter.Path).String() == filter.Value {
					matches++
				}
			}
			if matches == len(*filters) {
				return json.Unmarshal(val, obj)
			}
		}
		return nil
	})
}

// GetOneByFilterRaw returns the first raw value under "prefix:" matching filter.
func (store *Store) GetOneByFilterRaw(prefix string, filter Filter) (string, error) {
	var result string
	err := store.DB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		p := []byte(fmt.Sprintf("%s:", prefix))
		for it.Seek(p); it.ValidForPrefix(p); it.Next() {
			val, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if gjson.GetBytes(val, filter.Path).String() == filter.Value {
				result = string(val)
				break
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return result, nil
}

// DeleteByPrefix deletes every key under prefix. Keys are collected in a read
// transaction first, then deleted, to avoid iterating while mutating.
func (store *Store) DeleteByPrefix(prefix string) error {
	store.deleteOps.Add(1)
	var keysForDelete [][]byte
	err := store.DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		p := []byte(prefix)
		for it.Seek(p); it.ValidForPrefix(p); it.Next() {
			keysForDelete = append(keysForDelete, it.Item().KeyCopy(nil))
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(keysForDelete) > 0 {
		return store.DeleteKeys(keysForDelete)
	}
	return nil
}

// DeleteKeys deletes the given keys, committing and starting a new
// transaction whenever badger reports ErrTxnTooBig.
func (store *Store) DeleteKeys(keysForDelete [][]byte) error {
	txn := store.DB.NewTransaction(true)
	// Discard whatever transaction is current when we return. Assigning txn to
	// a fresh transaction below and discarding via the closure ensures the
	// replacement transaction is cleaned up too, not just the first one.
	defer func() { txn.Discard() }()
	for _, key := range keysForDelete {
		err := txn.Delete(key)
		if err == badger.ErrTxnTooBig {
			store.Logger.Info("got badger.ErrTxnTooBig - committing and creating new transaction")
			if err := txn.Commit(); err != nil {
				return err
			}
			txn = store.DB.NewTransaction(true)
			err = txn.Delete(key)
		}
		if err != nil {
			return err
		}
	}
	return txn.Commit()
}

// DeleteByMeta deletes every key whose user meta byte equals meta, then runs
// value-log garbage collection.
func (store *Store) DeleteByMeta(meta byte) error {
	var keysForDelete [][]byte
	err := store.DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if it.Item().UserMeta() == meta {
				keysForDelete = append(keysForDelete, it.Item().KeyCopy(nil))
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(keysForDelete) > 0 {
		if err := store.DeleteKeys(keysForDelete); err != nil {
			return err
		}
	}
	return store.GarbageCollection()
}

// DeleteOneByKey deletes a single key.
func (store *Store) DeleteOneByKey(key string) error {
	store.deleteOps.Add(1)
	return store.DB.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// GarbageCollection runs one round of value-log GC. Badger's "nothing to
// rewrite" result is not an error and is not reported as one.
func (store *Store) GarbageCollection() error {
	err := store.DB.RunValueLogGC(0.5)
	if errors.Is(err, badger.ErrNoRewrite) {
		return nil
	}
	return err
}

// StartGC runs value-log garbage collection on the given interval until ctx is
// cancelled, returning immediately (GC runs on its own goroutine). Badger only
// reclaims value-log space when RunValueLogGC is called, so a long-running
// service should start this once after opening the store. Each tick runs GC
// repeatedly until badger reports nothing left to rewrite.
func (store *Store) StartGC(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for {
					err := store.DB.RunValueLogGC(0.5)
					if err == nil {
						continue // reclaimed a file; keep going
					}
					if !errors.Is(err, badger.ErrNoRewrite) {
						store.Logger.Warn("store: value-log GC error: " + err.Error())
					}
					break
				}
			}
		}
	}()
}

// GetMetrics returns basic size metrics. Prefer CollectMetrics.
func (store *Store) GetMetrics() *Metrics {
	return NewMetrics(store.DB)
}

func matchesFilters(val []byte, filters []*Filter) bool {
	for _, filter := range filters {
		if gjson.GetBytes(val, filter.Path).String() != filter.Value {
			return false
		}
	}
	return true
}

// inTimeRange expects keys of the form "prefix:startMillis:endMillis".
func inTimeRange(key string, timeRange TimeRange) bool {
	kfields := strings.Split(key, ":")
	parse := func(i int) (time.Time, bool) {
		if i >= len(kfields) {
			return time.Time{}, false
		}
		ms, err := strconv.ParseInt(kfields[i], 10, 64)
		if err != nil {
			return time.Time{}, false
		}
		return time.UnixMilli(ms), true
	}
	hasStart := timeRange.Start != time.Time{}
	hasEnd := timeRange.End != time.Time{}
	if hasStart {
		startTime, ok := parse(1)
		if !ok || startTime.Before(timeRange.Start) {
			return false
		}
	}
	if hasEnd {
		endTime, ok := parse(2)
		if !ok || endTime.After(timeRange.End) {
			return false
		}
	}
	return hasStart || hasEnd
}

// validScopeComponent reports whether s is safe to use as an account ID or
// prefix in a scoped key. These must not contain ':' because that delimiter
// separates the {accountID}:{prefix}:{key} segments; a colon in either would
// shift the segment boundaries and could cross a tenant scope. The trailing key
// segment may contain colons (time-series keys use "lat:lon:ts"), so only the
// account ID and prefix are validated.
func validScopeComponent(s string) bool {
	return s != "" && !strings.Contains(s, ":")
}

// errInvalidScope is returned when an account ID or prefix is empty or contains
// a ':' delimiter.
var errInvalidScope = errors.New("store: account ID and prefix must be non-empty and contain no ':'")

// ScopedKey builds an account-scoped key: {accountID}:{prefix}:{key}
func ScopedKey(accountID, prefix, key string) string {
	return fmt.Sprintf("%s:%s:%s", accountID, prefix, key)
}

// ScopedPrefix builds an account-scoped prefix for scanning: {accountID}:{prefix}:
func ScopedPrefix(accountID, prefix string) string {
	return fmt.Sprintf("%s:%s:", accountID, prefix)
}

// PutScoped stores a value with an account-scoped key.
func (store *Store) PutScoped(accountID, prefix, key string, obj interface{}) error {
	if !validScopeComponent(accountID) || !validScopeComponent(prefix) {
		return errInvalidScope
	}
	return store.PutOne(ScopedKey(accountID, prefix, key), obj)
}

// PutScopedWithTTL stores a value with an account-scoped key and TTL.
func (store *Store) PutScopedWithTTL(accountID, prefix, key string, obj interface{}, ttl time.Duration) error {
	if !validScopeComponent(accountID) || !validScopeComponent(prefix) {
		return errInvalidScope
	}
	return store.PutOneWithTTL(ScopedKey(accountID, prefix, key), obj, ttl)
}

// GetScoped retrieves a value by account-scoped key.
func (store *Store) GetScoped(accountID, prefix, key string, obj interface{}) error {
	if !validScopeComponent(accountID) || !validScopeComponent(prefix) {
		return errInvalidScope
	}
	return store.GetOne(ScopedKey(accountID, prefix, key), obj)
}

// DeleteScoped deletes a value by account-scoped key.
func (store *Store) DeleteScoped(accountID, prefix, key string) error {
	if !validScopeComponent(accountID) || !validScopeComponent(prefix) {
		return errInvalidScope
	}
	return store.DeleteOneByKey(ScopedKey(accountID, prefix, key))
}

// ListScoped returns all values for an account-scoped prefix.
func (store *Store) ListScoped(accountID, prefix string, v interface{}) error {
	if !validScopeComponent(accountID) || !validScopeComponent(prefix) {
		return errInvalidScope
	}
	return store.Get(ScopedPrefix(accountID, prefix), v)
}

// ListScopedWithQuery returns values for an account-scoped prefix with query filters.
func (store *Store) ListScopedWithQuery(accountID string, query *Query, v interface{}) error {
	if !validScopeComponent(accountID) || !validScopeComponent(query.Prefix) {
		return errInvalidScope
	}
	scopedQuery := &Query{
		Prefix:    ScopedPrefix(accountID, query.Prefix),
		Filters:   query.Filters,
		Limit:     query.Limit,
		Offset:    query.Offset,
		TimeRange: query.TimeRange,
	}
	return store.GetWithQuery(scopedQuery, v)
}

// GetScopedStream streams values for an account-scoped prefix.
func (store *Store) GetScopedStream(w io.Writer, wg *sync.WaitGroup, accountID string, query *Query) {
	store.GetStream(w, wg, scopedQueryNoTrailingColon(accountID, query))
}

// GetScopedCount returns the count of items for an account-scoped prefix.
func (store *Store) GetScopedCount(accountID string, query *Query) (int, error) {
	if !validScopeComponent(accountID) || !validScopeComponent(query.Prefix) {
		return 0, errInvalidScope
	}
	return store.GetCount(scopedQueryNoTrailingColon(accountID, query))
}

// DeleteScopedByPrefix deletes all values for an account-scoped prefix.
func (store *Store) DeleteScopedByPrefix(accountID, prefix string) error {
	if !validScopeComponent(accountID) || !validScopeComponent(prefix) {
		return errInvalidScope
	}
	return store.DeleteByPrefix(ScopedPrefix(accountID, prefix))
}

// GetScopedByFilter retrieves a single value by account-scoped prefix and filters.
func (store *Store) GetScopedByFilter(accountID, prefix string, filters *[]Filter, obj interface{}) error {
	if !validScopeComponent(accountID) || !validScopeComponent(prefix) {
		return errInvalidScope
	}
	return store.GetOneByFilter(fmt.Sprintf("%s:%s", accountID, prefix), filters, obj)
}

// GetStream and GetCount append ":" themselves, so the scoped prefix they
// receive must not already end with one.
func scopedQueryNoTrailingColon(accountID string, query *Query) *Query {
	return &Query{
		Prefix:    fmt.Sprintf("%s:%s", accountID, query.Prefix),
		Filters:   query.Filters,
		Limit:     query.Limit,
		Offset:    query.Offset,
		TimeRange: query.TimeRange,
	}
}
