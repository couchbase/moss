//  Copyright (c) 2016 Marty Schoch

//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package test

import (
	"fmt"
	"sort"

	"github.com/couchbase/moss"

	"github.com/mschoch/smat"
)

// TODO: Test pre-allocated batches and AllocSet/Del/Merge().

// Fuzz using state machine driven by byte stream.
func Fuzz(data []byte) int {
	return smat.Fuzz(&context{}, smat.ActionID('S'), smat.ActionID('T'),
		actionMap, data)
}

type context struct {
	coll       moss.Collection // Initialized in setupFunc().
	collMirror mirrorColl

	mo moss.MergeOperator

	curBatch    int
	curSnapshot int
	curIterator int
	curKey      int

	batches      []moss.Batch
	batchMirrors []map[string]batchOp // Mirrors the entries in batches.

	snapshots       []moss.Snapshot
	snapshotMirrors []*mirrorColl

	iterators       []moss.Iterator
	iteratorMirrors []*mirrorIter

	keys []string
}

type batchOp struct {
	op, v string
}

type mirrorColl struct { // Used to validate coll and snapshot entries.
	kvs  map[string]string
	keys []string // Will be nil unless this is a snapshot.
}

type mirrorIter struct { // Used to validate iterator entries.
	pos int
	ss  *mirrorColl
}

// ------------------------------------------------------------------

var actionMap = smat.ActionMap{
	smat.ActionID('.'): delta(func(c *context) { c.curBatch++ }),
	smat.ActionID(','): delta(func(c *context) { c.curBatch-- }),
	smat.ActionID('{'): delta(func(c *context) { c.curSnapshot++ }),
	smat.ActionID('}'): delta(func(c *context) { c.curSnapshot-- }),
	smat.ActionID('['): delta(func(c *context) { c.curIterator++ }),
	smat.ActionID(']'): delta(func(c *context) { c.curIterator-- }),
	smat.ActionID(':'): delta(func(c *context) { c.curKey++ }),
	smat.ActionID(';'): delta(func(c *context) { c.curKey-- }),
	smat.ActionID('s'): opSetFunc,
	smat.ActionID('d'): opDelFunc,
	smat.ActionID('m'): opMergeFunc,
	smat.ActionID('g'): opGetFunc,
	smat.ActionID('B'): batchCreateFunc,
	smat.ActionID('b'): batchExecuteFunc,
	smat.ActionID('H'): snapshotCreateFunc,
	smat.ActionID('h'): snapshotCloseFunc,
	smat.ActionID('I'): iteratorCreateFunc,
	smat.ActionID('i'): iteratorCloseFunc,
	smat.ActionID('>'): iteratorNextFunc,
	smat.ActionID('K'): keyRegisterFunc,
	smat.ActionID('k'): keyUnregisterFunc,
}

var runningPercentActions []smat.PercentAction

func init() {
	pct := 100 / len(actionMap)
	for actionId := range actionMap {
		runningPercentActions = append(runningPercentActions,
			smat.PercentAction{pct, actionId})
	}

	actionMap[smat.ActionID('S')] = setupFunc
	actionMap[smat.ActionID('T')] = teardownFunc
}

// We only have one state: running.
func running(next byte) smat.ActionID {
	// Code coverage climbs maybe slightly faster if we do a simple modulus instead.
	//
	// return smat.PercentExecute(next, runningPercentActions...)
	//
	return runningPercentActions[int(next)%len(runningPercentActions)].Action
}

// Creates an action func based on a callback, used for moving the curXxxx properties.
func delta(cb func(c *context)) func(ctx smat.Context) (next smat.State, err error) {
	return func(ctx smat.Context) (next smat.State, err error) {
		c := ctx.(*context)
		cb(c)
		if c.curBatch < 0 {
			c.curBatch = 1000
		}
		if c.curSnapshot < 0 {
			c.curSnapshot = 1000
		}
		if c.curIterator < 0 {
			c.curIterator = 1000
		}
		if c.curKey < 0 {
			c.curKey = 1000
		}
		return running, nil
	}
}

// ------------------------------------------------------------------

func setupFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)

	mo := &moss.MergeOperatorStringAppend{Sep: ":"}

	coll, err := moss.NewCollection(moss.CollectionOptions{
		MergeOperator: mo,
	})
	if err != nil {
		return nil, err
	}
	c.coll = coll
	c.coll.Start()
	c.collMirror.kvs = map[string]string{}
	c.mo = mo

	return running, nil
}

func teardownFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	c.coll.Close()

	return nil, nil
}

// ------------------------------------------------------------------

func opSetFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	b, mirror, err := c.getCurBatch()
	if err != nil {
		return nil, err
	}
	k := c.getCurKey()
	_, exists := mirror[k]
	if !exists {
		mirror[k] = batchOp{op: "set", v: k}
		err := b.Set([]byte(k), []byte(k))
		if err != nil {
			return nil, err
		}
	}
	return running, nil
}

func opDelFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	b, mirror, err := c.getCurBatch()
	if err != nil {
		return nil, err
	}
	k := c.getCurKey()
	_, exists := mirror[k]
	if !exists {
		mirror[k] = batchOp{op: "del"}
		err := b.Del([]byte(k))
		if err != nil {
			return nil, err
		}
	}
	return running, nil
}

func opMergeFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	b, mirror, err := c.getCurBatch()
	if err != nil {
		return nil, err
	}
	k := c.getCurKey()
	_, exists := mirror[k]
	if !exists {
		mirror[k] = batchOp{op: "merge", v: k}
		err := b.Merge([]byte(k), []byte(k))
		if err != nil {
			return nil, err
		}
	}
	return running, nil
}

func opGetFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	ss, ssMirror, err := c.getCurSnapshot()
	if err != nil {
		return nil, err
	}
	k := c.getCurKey()
	v, err := ss.Get([]byte(k), moss.ReadOptions{})
	if err != nil {
		return nil, err
	}
	mirrorV := ssMirror.kvs[k]
	if string(v) != mirrorV {
		return nil, fmt.Errorf("get mismatch, got: %s, mirror: %s", v, mirrorV)
	}
	return running, nil
}

func batchCreateFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	b, err := c.coll.NewBatch(0, 0)
	if err != nil {
		return nil, err
	}
	c.batches = append(c.batches, b)
	c.batchMirrors = append(c.batchMirrors, map[string]batchOp{})
	return running, nil
}

func batchExecuteFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	if len(c.batches) <= 0 {
		return running, nil
	}
	b := c.batches[c.curBatch%len(c.batches)]
	err = c.coll.ExecuteBatch(b, moss.WriteOptions{})
	if err != nil {
		return nil, err
	}
	err = b.Close()
	if err != nil {
		return nil, err
	}
	batchOps := c.batchMirrors[c.curBatch%len(c.batchMirrors)]
	for key, batchOp := range batchOps {
		if batchOp.op == "set" {
			c.collMirror.kvs[key] = batchOp.v
		} else if batchOp.op == "del" {
			delete(c.collMirror.kvs, key)
		} else if batchOp.op == "merge" {
			k := []byte(key)
			v := []byte(batchOp.v)
			mv, ok := c.mo.FullMerge(k, []byte(c.collMirror.kvs[key]), [][]byte{v})
			if !ok {
				return nil, fmt.Errorf("failed FullMerge")
			}
			c.collMirror.kvs[key] = string(mv)
		} else {
			return nil, fmt.Errorf("unexpected batchOp.op: %+v, key: %s", batchOp, key)
		}
	}
	i := c.curBatch % len(c.batches)
	c.batches = append(c.batches[:i], c.batches[i+1:]...)
	i = c.curBatch % len(c.batchMirrors)
	c.batchMirrors = append(c.batchMirrors[:i], c.batchMirrors[i+1:]...)
	return running, nil
}

func snapshotCreateFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	ss, err := c.coll.Snapshot()
	if err != nil {
		return nil, err
	}
	c.snapshots = append(c.snapshots, ss)
	c.snapshotMirrors = append(c.snapshotMirrors, c.collMirror.snapshot())
	return running, nil
}

func snapshotCloseFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	if len(c.snapshots) <= 0 {
		return running, nil
	}
	i := c.curSnapshot % len(c.snapshots)
	ss := c.snapshots[i]
	err = ss.Close()
	if err != nil {
		return nil, err
	}
	c.snapshots = append(c.snapshots[:i], c.snapshots[i+1:]...)
	c.snapshotMirrors = append(c.snapshotMirrors[:i], c.snapshotMirrors[i+1:]...)
	return running, nil
}

func iteratorCreateFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	ss, ssMirror, err := c.getCurSnapshot()
	if err != nil {
		return nil, err
	}
	iter, err := ss.StartIterator(nil, nil, moss.IteratorOptions{})
	if err != nil {
		return nil, err
	}
	c.iterators = append(c.iterators, iter)
	c.iteratorMirrors = append(c.iteratorMirrors, &mirrorIter{ss: ssMirror})
	return running, nil
}

func iteratorCloseFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	if len(c.iterators) <= 0 {
		return running, nil
	}
	i := c.curIterator % len(c.iterators)
	iter := c.iterators[i]
	err = iter.Close()
	if err != nil {
		return nil, err
	}
	c.iterators = append(c.iterators[:i], c.iterators[i+1:]...)
	c.iteratorMirrors = append(c.iteratorMirrors[:i], c.iteratorMirrors[i+1:]...)
	return running, nil
}

func iteratorNextFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	if len(c.iterators) <= 0 {
		return running, nil
	}
	i := c.curIterator % len(c.iterators)
	iter := c.iterators[i]
	iterMirror := c.iteratorMirrors[i]
	err = iter.Next()
	iterMirror.pos++
	if err != nil && err != moss.ErrIteratorDone {
		return nil, err
	}
	k, v, err := iter.Current()
	if err != nil {
		if err != moss.ErrIteratorDone {
			return nil, err
		}
		if iterMirror.pos < len(iterMirror.ss.keys) {
			return nil, fmt.Errorf("iter done but iterMirror not done")
		}
	} else {
		if iterMirror.pos >= len(iterMirror.ss.keys) {
			return nil, fmt.Errorf("iterMirror done but iter not done")
		}
		iterMirrorKey := iterMirror.ss.keys[iterMirror.pos]
		if string(k) != iterMirrorKey {
			return nil, fmt.Errorf("iterMirror key != iter key")
		}
		if string(v) != iterMirror.ss.kvs[iterMirrorKey] {
			return nil, fmt.Errorf("iterMirror val != iter val")
		}
	}
	return running, nil
}

func keyRegisterFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	c.keys = append(c.keys, fmt.Sprintf("%d", c.curKey+len(c.keys)))
	return running, nil
}

func keyUnregisterFunc(ctx smat.Context) (next smat.State, err error) {
	c := ctx.(*context)
	if len(c.keys) <= 0 {
		return running, nil
	}
	i := c.curKey % len(c.keys)
	c.keys = append(c.keys[:i], c.keys[i+1:]...)
	return running, nil
}

// ------------------------------------------------------

func (c *context) getCurKey() string {
	if len(c.keys) <= 0 {
		return "x"
	}
	return c.keys[c.curKey%len(c.keys)]
}

func (c *context) getCurBatch() (moss.Batch, map[string]batchOp, error) {
	if len(c.batches) <= 0 {
		_, err := batchCreateFunc(c)
		if err != nil {
			return nil, nil, err
		}
	}
	return c.batches[c.curBatch%len(c.batches)],
		c.batchMirrors[c.curBatch%len(c.batchMirrors)], nil
}

func (c *context) getCurSnapshot() (moss.Snapshot, *mirrorColl, error) {
	if len(c.snapshots) <= 0 {
		_, err := snapshotCreateFunc(c)
		if err != nil {
			return nil, nil, err
		}
	}
	return c.snapshots[c.curSnapshot%len(c.snapshots)],
		c.snapshotMirrors[c.curSnapshot%len(c.snapshotMirrors)], nil
}

// ------------------------------------------------------------------

func (mc *mirrorColl) snapshot() *mirrorColl {
	kvs := map[string]string{}
	keys := make([]string, 0, len(kvs))
	for k, v := range mc.kvs {
		kvs[k] = v
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return &mirrorColl{kvs: kvs, keys: keys}
}
