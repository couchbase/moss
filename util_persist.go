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
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Implementation of mock lower-level iterator, using map that's
// cloned and sorted on creation.
type TestPersisterIterator struct {
	pos     int
	kvpairs map[string][]byte // immutable.
	keys    []string          // immutable, sorted.
	endkey  string
}

// NewTestPersisterIterator returns an iterator, cloning the provided
// kvpairs.
func NewTestPersisterIterator(kvpairs map[string][]byte,
	startkey, endkey string) *TestPersisterIterator {
	rv := &TestPersisterIterator{
		kvpairs: kvpairs,
		endkey:  endkey,
	}
	for k := range rv.kvpairs {
		rv.keys = append(rv.keys, k)
	}
	sort.Strings(rv.keys)
	rv.pos = sort.SearchStrings(rv.keys, string(startkey))
	return rv
}

func (i *TestPersisterIterator) Close() error {
	i.kvpairs = nil
	i.keys = nil
	return nil
}

func (i *TestPersisterIterator) Next() error {
	i.pos++
	if i.pos >= len(i.keys) {
		return ErrIteratorDone
	}
	return nil
}

func (i *TestPersisterIterator) Current() ([]byte, []byte, error) {
	if i.pos >= len(i.keys) {
		return nil, nil, ErrIteratorDone
	}
	k := i.keys[i.pos]
	if i.endkey != "" && strings.Compare(k, i.endkey) >= 0 {
		return nil, nil, ErrIteratorDone
	}
	return []byte(k), i.kvpairs[k], nil
}

func (i *TestPersisterIterator) CurrentEx() (entryEx EntryEx,
	key, val []byte, err error) {
	k, v, err := i.Current()
	if err != nil {
		return EntryEx{OperationSet}, nil, nil, err
	}
	return EntryEx{OperationSet}, k, v, err
}

// Implementation of mock lower-level test persister, using a map
// that's cloned on updates and with key sorting whenever an iterator
// is needed.
type TestPersister struct {
	// stable snapshots through writes blocking reads
	mutex sync.RWMutex

	kvpairs map[string][]byte
}

// NewTestPersister returns a TestPersister instance that can be used
// to test lower-level persistence features.
func NewTestPersister() *TestPersister {
	return &TestPersister{
		kvpairs: map[string][]byte{},
	}
}

func (p *TestPersister) cloneLOCKED() *TestPersister {
	c := NewTestPersister()
	for k, v := range p.kvpairs {
		c.kvpairs[k] = v
	}
	return c
}

func (p *TestPersister) Close() error {
	// ensure any writes in progress finish
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.kvpairs = nil
	return nil
}

func (p *TestPersister) Get(key []byte,
	readOptions ReadOptions) ([]byte, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.kvpairs[string(key)], nil
}

func (p *TestPersister) StartIterator(
	startKeyInclusive, endKeyExclusive []byte,
	iteratorOptions IteratorOptions) (Iterator, error) {
	p.mutex.RLock() // closing iterator unlocks
	defer p.mutex.RUnlock()
	return NewTestPersisterIterator(p.cloneLOCKED().kvpairs,
		string(startKeyInclusive), string(endKeyExclusive)), nil
}

func (p *TestPersister) Update(higher Snapshot) (*TestPersister, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	c := p.cloneLOCKED()

	if higher != nil {
		iter, err := higher.StartIterator(nil, nil, IteratorOptions{
			IncludeDeletions: true,
			SkipLowerLevel:   true,
		})
		if err != nil {
			return nil, err
		}

		defer iter.Close()

		var readOptions ReadOptions

		for {
			ex, key, val, err := iter.CurrentEx()
			if err == ErrIteratorDone {
				break
			}
			if err != nil {
				return nil, err
			}

			switch ex.Operation {
			case OperationSet:
				c.kvpairs[string(key)] = val

			case OperationDel:
				delete(c.kvpairs, string(key))

			case OperationMerge:
				val, err = higher.Get(key, readOptions)
				if err != nil {
					return nil, err
				}

				if val != nil {
					c.kvpairs[string(key)] = val
				} else {
					delete(c.kvpairs, string(key))
				}

			default:
				return nil, fmt.Errorf("moss TestPersister, update,"+
					" unexpected operation, ex: %v", ex)
			}

			err = iter.Next()
			if err == ErrIteratorDone {
				break
			}
			if err != nil {
				return nil, err
			}
		}
	}

	return c, nil
}
