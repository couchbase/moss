//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package moss

import (
	"errors"
	"sync"
)

// ErrNoValidFooter is returned when a valid footer could not be found
// in a file.
var ErrNoValidFooter = errors.New("no-valid-footer")

// --------------------------------------------------------

// Store represents data persisted in a directory.
type Store struct {
	dir     string
	options *StoreOptions

	m            sync.Mutex // Protects the fields that follow.
	refs         int
	footer       *Footer
	nextFNameSeq int64

	totPersists    uint64
	totCompactions uint64
}

// StoreOptions are provided to OpenStore().
type StoreOptions struct {
	// CollectionOptions should be the same as used with
	// NewCollection().
	CollectionOptions CollectionOptions

	// CompactionPercentage determines when a compaction will run when
	// CompactionConcern is CompactionAllowed.  When the percentage of
	// ops between the non-base level and the base level is greater
	// than CompactionPercentage, then compaction will be run.
	CompactionPercentage float64

	// CompactionBufferPages is the number of pages to use for
	// compaction, where writes are buffered before flushing to disk.
	CompactionBufferPages int

	// CompactionSync of true means perform a file sync at the end of
	// compaction for additional safety.
	CompactionSync bool

	// OpenFile allows apps to optionally provide their own file
	// opening implementation.  When nil, os.OpenFile() is used.
	OpenFile OpenFile `json:"-"`

	// Log is a callback invoked when store needs to log a debug
	// message.  Optional, may be nil.
	Log func(format string, a ...interface{}) `json:"-"`

	// KeepFiles means that unused, obsoleted files will not be
	// removed during OpenStore().  Keeping old files might be useful
	// when diagnosing file corruption cases.
	KeepFiles bool
}

// DefaultStoreOptions are the default store options when the
// application hasn't provided a meaningful configuration value.
var DefaultStoreOptions = StoreOptions{
	CompactionBufferPages: 512,
}

// StorePersistOptions are provided to Store.Persist().
type StorePersistOptions struct {
	// NoSync means do not perform a file sync at the end of
	// persistence (before returning from the Store.Persist() method).
	// Using NoSync of true might provide better performance, but at
	// the cost of data safety.
	NoSync bool

	// CompactionConcern controls whether compaction is allowed or
	// forced as part of persistence.
	CompactionConcern CompactionConcern
}

type CompactionConcern int // See StorePersistOptions.CompactionConcern.

// CompactionDisable means no compaction.
var CompactionDisable = CompactionConcern(0)

// CompactionAllow means compaction decision is automated and based on
// the configed policy and parameters, such as CompactionPercentage.
var CompactionAllow = CompactionConcern(1)

// CompactionForce means compaction should be performed immediately.
var CompactionForce = CompactionConcern(2)

// --------------------------------------------------------

// SegmentLoc represents a persisted segment.
type SegmentLoc struct {
	Kind string // Used as the key for SegmentLoaders.

	KvsOffset uint64 // Byte offset within the file.
	KvsBytes  uint64 // Number of bytes for the persisted segment.kvs.

	BufOffset uint64 // Byte offset within the file.
	BufBytes  uint64 // Number of bytes for the persisted segment.buf.

	TotOpsSet  uint64
	TotOpsDel  uint64
	TotKeyByte uint64
	TotValByte uint64

	mref *mmapRef // Immutable and ephemeral / non-persisted.
}

// TotOps returns number of ops in a segment loc.
func (sloc *SegmentLoc) TotOps() int { return int(sloc.KvsBytes / 8 / 2) }

// --------------------------------------------------------

type SegmentLocs []SegmentLoc

func (slocs SegmentLocs) AddRef() {
	for _, sloc := range slocs {
		if sloc.mref != nil {
			sloc.mref.AddRef()
		}
	}
}

func (slocs SegmentLocs) DecRef() {
	for _, sloc := range slocs {
		if sloc.mref != nil {
			sloc.mref.DecRef()
		}
	}
}

func (slocs SegmentLocs) Close() error {
	slocs.DecRef()
	return nil
}

// --------------------------------------------------------

// A SegmentLoaderFunc is able to load a segment from a SegmentLoc.
type SegmentLoaderFunc func(
	sloc *SegmentLoc, kvs []uint64, buf []byte) (Segment, error)

// SegmentLoaders is a registry of available segment loaders, which
// should be immutable after process init()'ialization.  It is keyed
// by SegmentLoc.Kind.
var SegmentLoaders = map[string]SegmentLoaderFunc{}

// --------------------------------------------------------

// OpenStore returns a store instance for a directory.  An empty
// directory results in an empty store.
func OpenStore(dir string, options StoreOptions) (*Store, error) {
	return openStore(dir, options)
}

func (s *Store) Dir() string {
	return s.dir
}

func (s *Store) Options() StoreOptions {
	return *s.options // Copy.
}

func (s *Store) Snapshot() (Snapshot, error) {
	return s.snapshot()
}

func (s *Store) snapshot() (*Footer, error) {
	s.m.Lock()
	footer := s.footer
	if footer != nil {
		footer.AddRef()
	}
	s.m.Unlock()
	return footer, nil
}

func (s *Store) AddRef() {
	s.m.Lock()
	s.refs++
	s.m.Unlock()
}

func (s *Store) Close() error {
	s.m.Lock()

	s.refs--
	if s.refs > 0 {
		s.m.Unlock()
		return nil
	}

	footer := s.footer
	s.footer = nil

	s.m.Unlock()

	return footer.Close()
}

// --------------------------------------------------------

// Persist helps the store implement the lower-level-update func.  The
// higher snapshot may be nil.
func (s *Store) Persist(higher Snapshot, persistOptions StorePersistOptions) (
	Snapshot, error) {
	return s.persist(higher, persistOptions)
}

// --------------------------------------------------------

// OpenStoreCollection returns collection based on a persisted store
// in a directory.  Updates to the collection will be persisted.  An
// empty directory starts an empty collection.  Both the store and
// collection should be closed by the caller when done.
func OpenStoreCollection(dir string,
	options StoreOptions,
	persistOptions StorePersistOptions) (*Store, Collection, error) {
	store, err := OpenStore(dir, options)
	if err != nil {
		return nil, nil, err
	}

	coll, err := store.OpenCollection(options, persistOptions)
	if err != nil {
		store.Close()
		return nil, nil, err
	}

	return store, coll, nil
}

// --------------------------------------------------------

// OpenCollection opens a collection based on a store.  Applications
// should open at most a single collection per store for performing
// read/write work.
func (store *Store) OpenCollection(
	options StoreOptions,
	persistOptions StorePersistOptions) (Collection, error) {
	return store.openCollection(options, persistOptions)
}

// --------------------------------------------------------

// SnapshotPrevious returns the next older, previous snapshot based on
// a given snapshot, allowing the application to walk backwards into
// the history of a store at previous points in time.  The given
// snapshot must come from the same store.  A nil returned snapshot
// means no previous snapshot is available.  Of note, store
// compactions will trim previous history from a store.
func (s *Store) SnapshotPrevious(ss Snapshot) (Snapshot, error) {
	return s.snapshotPrevious(ss)
}

// --------------------------------------------------------

// SnapshotRevert atomically and durably brings the store back to the
// point-in-time as represented by the revertTo snapshot.
// SnapshotRevert() should only be passed a snapshot that came from
// the same store, such as from using Store.Snapshot() or
// Store.SnapshotPrevious().
//
// SnapshotRevert() must not be invoked concurrently with
// Store.Persist(), so it is recommended that SnapshotRevert() should
// be invoked only after the collection has been Close()'ed, which
// helps ensure that you are not racing with concurrent, background
// persistence goroutines.
//
// SnapshotRevert() can fail if the given snapshot is too old,
// especially w.r.t. compactions.  For example, navigate back to an
// older snapshot X via SnapshotPrevious().  Then, do a full
// compaction.  Then, SnapshotRevert(X) will give an error.
func (s *Store) SnapshotRevert(revertTo Snapshot) error {
	return s.snapshotRevert(revertTo)
}
