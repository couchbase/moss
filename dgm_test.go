package moss

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"
)

// TODO: This is copied code from cbft. Need to make mossHerder reusable.
type mossHerder struct {
	memQuota uint64

	m        sync.Mutex // Protects the fields that follow.
	waitCond *sync.Cond
	waiting  int

	// The map tracks moss collections currently being herded
	collections map[Collection]struct{}
}

// newMossHerder returns a new moss herder instance.
func newMossHerder(memQuota uint64) *mossHerder {
	mh := &mossHerder{
		memQuota:    memQuota,
		collections: map[Collection]struct{}{},
	}
	mh.waitCond = sync.NewCond(&mh.m)
	return mh
}

// NewMossHerderOnEvent returns a func closure that that can be used
// as a moss OnEvent() callback.
func NewMossHerderOnEvent(memQuota uint64) func(Event) {
	if memQuota <= 0 {
		return nil
	}

	mh := newMossHerder(memQuota)

	return func(event Event) { mh.OnEvent(event) }
}

func (mh *mossHerder) OnEvent(event Event) {
	switch event.Kind {
	case EventKindCloseStart:
		mh.OnCloseStart(event.Collection)

	case EventKindClose:
		mh.OnClose(event.Collection)

	case EventKindBatchExecuteStart:
		mh.OnBatchExecuteStart(event.Collection)

	case EventKindPersisterProgress:
		mh.OnPersisterProgress(event.Collection)

	default:
		return
	}
}

func (mh *mossHerder) OnCloseStart(c Collection) {
	mh.m.Lock()

	if mh.waiting > 0 {
		fmt.Printf("moss_herder: close start progress, waiting: %d", mh.waiting)
	}

	delete(mh.collections, c)

	mh.m.Unlock()
}

func (mh *mossHerder) OnClose(c Collection) {
	mh.m.Lock()

	if mh.waiting > 0 {
		fmt.Printf("moss_herder: close progress, waiting: %d", mh.waiting)
	}

	delete(mh.collections, c)

	mh.m.Unlock()
}

func (mh *mossHerder) OnBatchExecuteStart(c Collection) {
	if c.Options().LowerLevelUpdate == nil {
		return
	}

	mh.m.Lock()

	mh.collections[c] = struct{}{}

	for mh.overMemQuotaLOCKED() {
		// If we're over the memory quota, then wait for persister progress.
		mh.waiting++
		mh.waitCond.Wait()
		fmt.Printf("moss_herder: waiting for persistence..\n")
		mh.waiting--
	}

	mh.m.Unlock()
}

func (mh *mossHerder) OnPersisterProgress(c Collection) {
	if c.Options().LowerLevelUpdate == nil {
		return
	}

	mh.m.Lock()

	if mh.waiting > 0 {
		fmt.Printf("moss_herder: persistence progress, woken: %d\n", mh.waiting)
	}

	mh.waitCond.Broadcast()

	mh.m.Unlock()
}

// --------------------------------------------------------

// overMemQuotaLOCKED() returns true if the number of dirty bytes is
// greater than the memory quota.
func (mh *mossHerder) overMemQuotaLOCKED() bool {
	var totDirtyBytes uint64

	for c := range mh.collections {
		s, err := c.Stats()
		if err != nil {
			fmt.Printf("moss_herder: stats, err: %v\n", err)
			continue
		}

		totDirtyBytes += s.CurDirtyBytes
	}

	return totDirtyBytes > mh.memQuota
}

var numitems = flag.Int("numItems", 100000, "number of items to load")
var batchsize = flag.Int("batchSize", 100, "number of items per batch")
var memquota = flag.Uint64("memQuota", 128*1024*1024, "Memory quota")

func Test_DGMLoad(t *testing.T) {
	numItems := 400000
	batchSize := 100
	var memQuota uint64 = 128 * 1024 * 1024
	flag.Parse()
	if numitems != nil {
		numItems = *numitems
	}
	if batchsize != nil {
		batchSize = *batchsize
	}
	if memquota != nil {
		memQuota = *memquota
	}

	tmpDir, _ := ioutil.TempDir("", "mossStoreDGM")
	defer os.RemoveAll(tmpDir)

	so := DefaultStoreOptions
	so.CollectionOptions.MinMergePercentage = 0.0
	so.CollectionOptions.OnEvent = NewMossHerderOnEvent(memQuota)
	so.CompactionPercentage = 1.0
	so.CompactionSync = true
	spo := StorePersistOptions{CompactionConcern: CompactionAllow} //, AsyncCompaction: true}

	store, coll, err := OpenStoreCollection(tmpDir, so, spo)
	if err != nil || store == nil || coll == nil {
		t.Fatalf("error opening store collection:%v", tmpDir)
	}
	startT := time.Now()
	startI := 0
	bigVal := fmt.Sprintf("%0504d", numItems)

	for i := 0; i <= numItems; i = i + batchSize {
		// create new batch to set some keys
		ba, errr := coll.NewBatch(batchSize, batchSize*512)
		if errr != nil {
			t.Fatalf("error creating new batch: %v", err)
			return
		}

		for j := i + batchSize - 1; j > i; j-- {
			k := fmt.Sprintf("%08d", j)
			ba.Set([]byte(k), []byte(bigVal))
		}
		err = coll.ExecuteBatch(ba, WriteOptions{})
		if err != nil {
			t.Fatalf("error executing batch: %v", err)
			return
		}

		// cleanup that batch
		err = ba.Close()
		if err != nil {
			t.Fatalf("error closing batch: %v", err)
			return
		}
		elapsed := time.Since(startT)
		if elapsed > time.Second {
			fmt.Printf("Loaded items upto %d. Rate %f KOps/s\n", i,
				((float64)(i-startI))/elapsed.Seconds()/1000)
			startI = i
			startT = time.Now()
		}
	}

	val, erro := coll.Get([]byte(fmt.Sprintf("%08d", numItems-1)), ReadOptions{})
	if erro != nil || val == nil {
		t.Fatalf("Unable to fetch the key written! %v", err)
	}
	// Bug - if we remove this Sleep, we hit data loss
	time.Sleep(2 * time.Second)

	if store.Close() != nil {
		t.Fatalf("expected store close to work")
	}

	if coll.Close() != nil {
		t.Fatalf("Error closing child collection")
	}

	store, coll, err = OpenStoreCollection(tmpDir, so, spo)
	if err != nil || store == nil || coll == nil {
		t.Fatalf("error opening store collection:%v", tmpDir)
	}
	for i := numItems - 1; i > 0; i-- {
		key := fmt.Sprintf("%08d", i)
		val, erro = coll.Get([]byte(key), ReadOptions{})
		if erro != nil || val == nil {
			fmt.Println("Data loss for item ", key)
		} else {
			break
		}
	}

	if store.Close() != nil {
		t.Fatalf("expected store close to work")
	}

	if coll.Close() != nil {
		t.Fatalf("Error closing child collection")
	}
}
