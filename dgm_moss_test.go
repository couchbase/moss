package moss

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TODO: This is copied code from cbft. Need to make mossHerderDGM reusable.
type mossHerderDGM struct {
	memQuota uint64

	m        sync.Mutex // Protects the fields that follow.
	waitCond *sync.Cond
	waiting  int

	// The map tracks moss collections currently being herded
	collections map[Collection]struct{}
	dgm         *dgmTest
}

// newMossHerderDGM returns a new moss herder instance.
func newMossHerderDGM(memQuota uint64) *mossHerderDGM {
	mh := &mossHerderDGM{
		memQuota:    memQuota,
		collections: map[Collection]struct{}{},
	}
	mh.waitCond = sync.NewCond(&mh.m)
	return mh
}

// NewMossHerderOnEventDGM returns a func closure that that can be used
// as a moss OnEvent() callback.
func NewMossHerderOnEventDGM(dgm *dgmTest) func(Event) {
	if dgm.memQuota <= 0 {
		return nil
	}

	mh := newMossHerderDGM(dgm.memQuota)
	mh.dgm = dgm
	dgm.mh = mh

	return func(event Event) { mh.OnEvent(event) }
}

func (mh *mossHerderDGM) OnEvent(event Event) {
	switch event.Kind {
	case EventKindCloseStart:
		mh.OnClose(event.Collection)

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

func (mh *mossHerderDGM) WaitForMossHerder(c Collection) {
	mh.m.Lock()
	for mh.waiting > 0 {
		mh.m.Unlock()
		tm := time.Now()
		fmt.Printf("%02d:%02d:%02d - MossHerder waiting: %d\n", tm.Hour(), tm.Minute(), tm.Second(), mh.waiting)
		time.Sleep(time.Second * 1)
		mh.m.Lock()
	}
	mh.m.Unlock()
}

func (mh *mossHerderDGM) OnClose(c Collection) {
	mh.WaitForMossHerder(c)
	mh.m.Lock()
	delete(mh.collections, c)
	mh.m.Unlock()
}

func (mh *mossHerderDGM) OnBatchExecuteStart(c Collection) {
	if c.Options().LowerLevelUpdate == nil {
		return
	}

	mh.m.Lock()

	mh.collections[c] = struct{}{}

	waited := false
	startTime := time.Now()
	for mh.overMemQuotaLOCKED() {
		if !waited {
			waited = true
			//fmt.Printf("%02d:%02d:%02d mossHeader(start)\n", startTime.Hour(), startTime.Minute(), startTime.Second())
		}

		// If we're over the memory quota, then wait for persister progress.
		mh.waiting++
		mh.waitCond.Wait()
		//fmt.Printf("moss_herder: waiting for persistence..\n")
		mh.waiting--
	}
	if waited {
		elapsed := time.Since(startTime)
		//t := time.Now()
		//fmt.Printf("%02d:%02d:%02d mossHeader(stop) duration %.3f\n", t.Hour(), t.Minute(), t.Second(), elapsed.Seconds())
		mh.dgm.mhBlockDuration += uint64(elapsed.Seconds() * 1000)
		mh.dgm.mhBlocks++
	}

	mh.m.Unlock()
}

func (mh *mossHerderDGM) OnPersisterProgress(c Collection) {
	if c.Options().LowerLevelUpdate == nil {
		return
	}

	mh.m.Lock()

	// if mh.waiting > 0 {
	// fmt.Printf("moss_herder: persistence progress, woken: %d\n", mh.waiting)
	// }

	mh.waitCond.Broadcast()

	mh.m.Unlock()
}

// --------------------------------------------------------

// overMemQuotaLOCKED() returns true if the number of dirty bytes is
// greater than the memory quota.
func (mh *mossHerderDGM) overMemQuotaLOCKED() bool {
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

type keyOrderT int

const (
	AscendingKO keyOrderT = 1 + iota
	RandomKO
)

var keyOrders = [...]string{
	"Ascending",
	"Random",
}

func (ko keyOrderT) String() string {
	return keyOrders[ko-1]
}

type dgmTest struct {
	collection     Collection
	store          *Store
	memQuota       uint64
	runDescription string
	dbCreate       bool
	dbSize         uint64
	dbPath         string
	runTime        time.Duration
	t              *testing.T

	keyLength    int
	keyLengthFmt string
	valueLength  int
	keyOrder     keyOrderT
	key          uint64
	keyLock      sync.Mutex
	startKey     uint64

	numWriters          int
	writeBatchSize      uint64
	writeBatchThinkTime time.Duration
	numKeysWrite        uint64
	numWriteBatches     uint64
	waitGroupWriters    sync.WaitGroup

	numReaders         int
	readBatchSize      uint64
	readBatchThinkTime time.Duration
	numReadBatches     uint64
	numKeysRead        uint64
	waitGroupReaders   sync.WaitGroup

	numKeysStart uint64

	sampleFrequency time.Duration
	waitGroupSample sync.WaitGroup
	diskMonitor     string
	outputToFile    bool

	workersStop     int32
	allStop         int32
	resultsFileName string

	mh              *mossHerderDGM
	mhBlockDuration uint64
	mhBlocks        uint64
}

func RandomString(r *rand.Rand, buf *bytes.Buffer, n int) {
	var letterRunes = []rune("0123456789abcdefABCDEF")

	for i := 0; i < n; i++ {
		buf.WriteRune(letterRunes[r.Intn(len(letterRunes))])
	}
	return
}

func (dgm *dgmTest) generateKV(r *rand.Rand, keyBuf *bytes.Buffer, valBuf *bytes.Buffer) {
	var k uint64
	var maxKey uint64

	switch dgm.keyOrder {
	case AscendingKO:
		k = atomic.AddUint64(&dgm.key, 1)
	case RandomKO:
		maxKey = atomic.LoadUint64(&dgm.numKeysWrite)
		k = (uint64)(r.Int63n((int64)(maxKey+10000))) + dgm.startKey
		if k > dgm.key {
			dgm.keyLock.Lock()
			if k > dgm.key {
				atomic.StoreUint64(&dgm.key, k)
			}
			dgm.keyLock.Unlock()
		}
	}

	keyBuf.Truncate(0)
	fmt.Fprintf(keyBuf, dgm.keyLengthFmt, k)

	valBuf.Truncate(0)
	RandomString(r, valBuf, dgm.valueLength)
	return
}

func (dgm *dgmTest) createItems() {

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	var keyBuf bytes.Buffer
	keyBuf.Grow(dgm.keyLength + 1)
	var valBuf bytes.Buffer
	valBuf.Grow(dgm.valueLength + 1)

	var keynum = dgm.startKey

	nItems := dgm.dbSize / (uint64)(dgm.keyLength+dgm.valueLength)
	for nItems > 0 {
		bs := (int)(math.Min((float64)(dgm.writeBatchSize), (float64)(nItems)))

		// create new batch to set some keys
		ba, err := dgm.collection.NewBatch(bs, bs*512)
		if err != nil {
			se := fmt.Sprintf("Error create NewBatch - %v", err)
			panic(se)
		}

		for i := 0; i < bs; i++ {
			keyBuf.Truncate(0)
			fmt.Fprintf(&keyBuf, dgm.keyLengthFmt, keynum)
			valBuf.Truncate(0)
			RandomString(r1, &valBuf, dgm.valueLength)

			ba.Set(keyBuf.Bytes(), valBuf.Bytes())
			//fmt.Printf("key %s\nval %s\n", keyBuf.String(), valBuf.String())

			keynum++
			atomic.AddUint64(&dgm.numKeysWrite, 1)
		}

		err = dgm.collection.ExecuteBatch(ba, WriteOptions{})
		if err != nil {
			es := fmt.Sprintf("Error ExecuteBatch - %v", err)
			panic(es)
		}

		err = ba.Close()
		if err != nil {
			es := fmt.Sprintf("Error ba.Close - %v", err)
			panic(es)
		}

		atomic.AddUint64(&dgm.numWriteBatches, 1)

		nItems -= (uint64)(bs)
	}

	dgm.startKey = keynum
	//fmt.Printf("createItems done - startKey %d\n", dgm.startKey)
}

func (dgm *dgmTest) addItems(workerID int) {
	defer dgm.waitGroupWriters.Done()

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	var keyBuf bytes.Buffer
	keyBuf.Grow(dgm.keyLength + 1)
	var valBuf bytes.Buffer
	valBuf.Grow(dgm.valueLength + 1)

	for atomic.LoadInt32(&dgm.workersStop) == 0 {
		// create new batch to set some keys
		ba, err := dgm.collection.NewBatch((int)(dgm.writeBatchSize), (int)(dgm.writeBatchSize*512))
		if err != nil {
			se := fmt.Sprintf("Error create NewBatch - %v", err)
			panic(se)
		}

		for i := 0; i < (int)(dgm.writeBatchSize); i++ {
			dgm.generateKV(r1, &keyBuf, &valBuf)

			ba.Set(keyBuf.Bytes(), valBuf.Bytes())

			//fmt.Printf("addItems %s - %s\n", keyBuf.String(), valBuf.String())

			atomic.AddUint64(&dgm.numKeysWrite, 1)
		}

		err = dgm.collection.ExecuteBatch(ba, WriteOptions{})
		if err != nil {
			es := fmt.Sprintf("Error ExecuteBatch - %v", err)
			panic(es)
		}

		err = ba.Close()
		if err != nil {
			es := fmt.Sprintf("Error ba.Close - %v", err)
			panic(es)
		}

		atomic.AddUint64(&dgm.numWriteBatches, 1)
		time.Sleep(dgm.writeBatchThinkTime)
	}
}

func (dgm *dgmTest) readItems(workerID int) {
	defer dgm.waitGroupReaders.Done()

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	var sk bytes.Buffer
	sk.Grow(dgm.keyLength + 1)
	var ek bytes.Buffer
	ek.Grow(dgm.keyLength + 1)

	for atomic.LoadInt32(&dgm.workersStop) == 0 {
		var minKey int64
		var maxKey = (int64)(atomic.LoadUint64(&dgm.key)) + (int64)(dgm.startKey)

		// if we have inserted any keys yet, stall the read a bit
		if minKey == maxKey {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		startKey := r1.Int63n((int64)(maxKey-minKey)) + minKey

		sk.Truncate(0)
		fmt.Fprintf(&sk, dgm.keyLengthFmt, startKey)

		ek.Truncate(0)
		fmt.Fprintf(&ek, dgm.keyLengthFmt, startKey+int64(dgm.readBatchSize))

		ss, err := dgm.collection.Snapshot()
		if err != nil || ss == nil {
			es := fmt.Sprintf("Create SnapShot failed - %v", err)
			panic(es)
		}

		iter, err := ss.StartIterator(sk.Bytes(), ek.Bytes(), IteratorOptions{})
		if err != nil || iter == nil {
			es := fmt.Sprintf("StartIterator failed - %v", err)
			panic(es)
		}

		nkeysRead := 0

		for {
			err = iter.Next()
			if err == ErrIteratorDone {
				break
			} else if err != nil {
				es := fmt.Sprintf("iter.Next failed - %v", err)
				panic(es)
			}
			k, v, err := iter.Current()
			if err == ErrIteratorDone {
				break
			} else if err != nil || k == nil || v == nil {
				es := fmt.Sprintf("iter.Current failed - %v", err)
				panic(es)
			}
			nkeysRead++
			//fmt.Printf("readItems\nkey %s\nval %s\n", k, v)
		}

		iter.Close()
		ss.Close()

		atomic.AddUint64(&dgm.numReadBatches, 1)
		atomic.AddUint64(&dgm.numKeysRead, uint64(nkeysRead))

		time.Sleep(dgm.readBatchThinkTime)
	}
}

func (dgm *dgmTest) getFooterStats() (uint64, uint64, uint64, uint64) {

	ss, err := dgm.store.Snapshot()
	if err != nil || ss == nil {
		se := fmt.Sprintf("Unable to get store.Snapshot - %v", err)
		panic(se)
	}

	var totalOpsSet uint64
	var totalOpsDel uint64
	var totalKeyBytes uint64
	var totalValBytes uint64
	for i := range ss.(*Footer).SegmentLocs {
		sloc := &ss.(*Footer).SegmentLocs[i]

		totalOpsSet += sloc.TotOpsSet
		totalOpsDel += sloc.TotOpsDel
		totalKeyBytes += sloc.TotKeyByte
		totalValBytes += sloc.TotValByte
	}
	ss.Close()

	return totalOpsSet, totalOpsDel, totalKeyBytes, totalValBytes
}

func (dgm *dgmTest) getDGMStats(lastSampleTime time.Duration) map[string]interface{} {
	stats := make(map[string]interface{})

	stats["numKeysWrite"] = atomic.LoadUint64(&dgm.numKeysWrite)
	stats["numWriteBatches"] = atomic.LoadUint64(&dgm.numWriteBatches)
	stats["numKeysRead"] = atomic.LoadUint64(&dgm.numKeysRead)
	stats["numReadBatches"] = atomic.LoadUint64(&dgm.numReadBatches)
	stats["numKeysStart"] = dgm.numKeysStart
	stats["mhBlockDuration"] = dgm.mhBlockDuration
	stats["mhBlocks"] = dgm.mhBlocks

	stats["totalOpsSet"], stats["totalOpsDel"], stats["totalKeyBytes"], stats["totalValBytes"] = dgm.getFooterStats()

	sstats, _ := dgm.store.Stats()
	for k, v := range sstats {
		switch k {
		case "num_files":
			val, ok := v.(int)
			if !ok {
				fmt.Printf("can't convert %v\n", v)
			} else {
				stats[k] = (uint64)(val)
			}
		case "total_persists", "num_segments", "num_bytes_used_disk", "total_compactions":
			stats[k] = v
		}
	}

	content, err := ioutil.ReadFile("/proc/stat")
	if err == nil {
		lines := strings.Split(string(content), "\n")
		fields := strings.Fields(lines[0])
		cpuUser, _ := strconv.ParseUint(fields[1], 0, 64)
		cpuSystem, _ := strconv.ParseUint(fields[3], 0, 64)
		cpuIdle, _ := strconv.ParseUint(fields[4], 0, 64)
		cpuIowait, _ := strconv.ParseUint(fields[5], 0, 64)

		stats["cpu_user"] = (uint64)(cpuUser)
		stats["cpu_system"] = (uint64)(cpuSystem)
		stats["cpu_idle"] = (uint64)(cpuIdle)
		stats["cpu_iowait"] = (uint64)(cpuIowait)
	}

	content, err = ioutil.ReadFile("/proc/diskstats")
	if err == nil {
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) < 14 {
				continue
			}
			if fields[2] == dgm.diskMonitor {
				stats["read_ios"], _ = strconv.ParseUint(fields[3], 0, 64)
				stats["read_sectors"], _ = strconv.ParseUint(fields[5], 0, 64)
				stats["read_ticks"], _ = strconv.ParseUint(fields[6], 0, 64)
				stats["write_ios"], _ = strconv.ParseUint(fields[7], 0, 64)
				stats["write_sectors"], _ = strconv.ParseUint(fields[9], 0, 64)
				stats["write_ticks"], _ = strconv.ParseUint(fields[10], 0, 64)
				stats["avq"], _ = strconv.ParseUint(fields[13], 0, 64)
				break
			}
		}
	}

	content, err = ioutil.ReadFile("/proc/meminfo")
	if err == nil {
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) < 3 {
				continue
			}
			val, _ := strconv.ParseInt(fields[1], 0, 64)
			val = val * 1024
			//fmt.Printf("%v - %d\n", fields, val)
			if strings.Contains(line, "MemTotal:") {
				stats["memtotal"] = (uint64)(val)
			} else if strings.Contains(line, "MemFree:") {
				stats["memfree"] = (uint64)(val)
			} else if strings.Contains(line, "Cached:") && !strings.Contains(line, "Swap") {
				stats["cached"] = (uint64)(val)
			} else if strings.Contains(line, "Mapped:") {
				stats["mapped"] = (uint64)(val)
			} else if strings.Contains(line, "Buffers:") {
				stats["buffers"] = (uint64)(val)
			}
		}
	} else {
		stats["processMem"] = (uint64)(0)
		stats["mapped"] = (uint64)(0)
	}

	content, err = ioutil.ReadFile("/proc/self/status")
	if err == nil {
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) < 3 {
				continue
			}
			val, _ := strconv.ParseInt(fields[1], 0, 64)
			val = val * 1024
			if strings.Contains(line, "VmRSS:") {
				stats["processMem"] = (uint64)(val)
				break
			}
		}
	}

	//fmt.Println("getDGMStats....")
	//fmt.Println(stats)

	return stats
}

func (dgm *dgmTest) sampleStats(firstStats map[string]interface{}) {
	defer dgm.waitGroupSample.Done()
	var outputFile *os.File

	if dgm.outputToFile {
		fileName := fmt.Sprintf("%s.json", dgm.resultsFileName)

		outputFile, _ = os.Create(fileName)
		defer outputFile.Close()

		title := make(map[string]interface{})

		title["cfg_CompactionPercentage"] = dgm.store.options.CompactionPercentage
		title["cfg_CompactionLevelMaxSegments"] = dgm.store.options.CompactionLevelMaxSegments
		title["cfg_CompactionLevelMultiplier"] = dgm.store.options.CompactionLevelMultiplier
		title["cfg_CompactionBufferPages"] = dgm.store.options.CompactionBufferPages
		title["runDescription"] = dgm.runDescription
		title["keyLength"] = dgm.keyLength
		title["valueLength"] = dgm.valueLength
		title["keyOrder"] = fmt.Sprintf("%s", dgm.keyOrder)
		title["dbPath"] = dgm.dbPath
		title["runTime"] = dgm.runTime.Seconds()
		title["sampleFrequency"] = dgm.sampleFrequency
		title["diskMonitor"] = dgm.diskMonitor
		title["numWriters"] = dgm.numWriters
		title["writeBatchSize"] = dgm.writeBatchSize
		title["writeBatchThinkTime"] = dgm.writeBatchThinkTime.Seconds()
		title["numReaders"] = dgm.numReaders
		title["readBatchSize"] = dgm.readBatchSize
		title["readBatchThinkTime"] = dgm.readBatchThinkTime.Seconds()
		title["memQuota"] = dgm.memQuota

		title["ncpus"] = runtime.NumCPU()

		jBuf, _ := json.Marshal(title)
		outputFile.WriteString(string(jBuf))
		outputFile.WriteString("\n")
	}

	lastSampleTime := time.Now()
	last := dgm.getDGMStats(time.Since(lastSampleTime))

	for atomic.LoadInt32(&dgm.allStop) == 0 {
		time.Sleep(dgm.sampleFrequency)
		intervalStats := make(map[string]interface{})

		curr := dgm.getDGMStats(time.Since(lastSampleTime))

		for k := range curr {
			switch k {
			case "memtotal", "memfree", "cached", "mapped", "buffers", "memory_quota", "num_segments", "num_files", "num_bytes_used_disk", "totalKeyBytes", "totalValBytes", "processMem":
				intervalStats[k] = curr[k]
			default:
				/* keep the changes that have occurred between intervals */
				curVal, _ := curr[k].(uint64)
				lastVal, _ := last[k].(uint64)
				if lastVal > curVal {
					intervalStats[k] = 0
				} else {
					intervalStats[k] = curVal - lastVal
				}
			}
		}

		t := time.Now()
		tBuf := fmt.Sprintf("%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())
		intervalStats["intervaltime"] = tBuf

		kb, _ := intervalStats["totalKeyBytes"].(uint64)
		vb, _ := intervalStats["totalValBytes"].(uint64)
		dbSize := kb + vb
		mossSize, _ := intervalStats["num_bytes_used_disk"].(uint64)
		processMem := (uint64)(intervalStats["processMem"].(uint64))
		memMappedFiles, _ := intervalStats["mapped"].(uint64)

		fmt.Printf("%s OpsSet %v numKeysRead %v dbSize %dmb %dmb Memory %dmb MMap %dmb\n", tBuf, intervalStats["totalOpsSet"], intervalStats["numKeysRead"], dbSize/1024/1024, mossSize/1024/1024, processMem/1024/1024, memMappedFiles/1024/1024)

		if dgm.outputToFile {
			jBuf, _ := json.Marshal(intervalStats)
			outputFile.WriteString(string(jBuf))
			outputFile.WriteString("\n")
		}

		last = curr
	}

	dgm.mh.WaitForMossHerder(dgm.collection)

	if dgm.outputToFile {
		// when we close and reopen the Collections, we lose
		// some stats so save them off here
		almostFinalStats := dgm.getDGMStats(time.Since(lastSampleTime))
		lastSampleTime = time.Now()

		// close and reopen Collections to get real final stats
		//
		dgm.closeMossStore()
		time.Sleep(time.Second * 5)
		dgm.openMossStore()

		finalStats := dgm.getDGMStats(time.Since(lastSampleTime))
		trailer := make(map[string]interface{})
		for k := range firstStats {
			curVal, _ := finalStats[k].(uint64)
			lastVal, _ := firstStats[k].(uint64)
			totKey := fmt.Sprintf("tot_%s", k)
			if lastVal > curVal {
				trailer[totKey] = 0
			} else {
				trailer[totKey] = curVal - lastVal
			}
		}
		trailer["tot_total_persists"] = almostFinalStats["total_persists"]
		trailer["tot_total_compactions"] = almostFinalStats["total_compactions"]
		trailer["tot_totalOpsSet"], trailer["tot_totalOpsDel"], trailer["tot_totalKeyBytes"], trailer["tot_totalValBytes"] = dgm.getFooterStats()

		jBuf, _ := json.Marshal(trailer)
		outputFile.WriteString(string(jBuf))
		outputFile.WriteString("\n")
	}

}

func parseSize(input string) uint64 {
	var retSize uint64

	input = strings.ToLower(input)

	if strings.Contains(input, "kb") {
		num, err := strconv.ParseUint(strings.Split(input, "kb")[0], 10, 64)
		if err != nil {
			return retSize
		}
		retSize = uint64(num) * 1024
	} else if strings.Contains(input, "mb") {
		num, err := strconv.ParseUint(strings.Split(input, "mb")[0], 10, 64)
		if err != nil {
			return retSize
		}
		retSize = uint64(num) * 1024 * 1024
	} else if strings.Contains(input, "gb") {
		num, err := strconv.ParseUint(strings.Split(input, "gb")[0], 10, 64)
		if err != nil {
			return retSize
		}
		retSize = uint64(num) * 1024 * 1024 * 1024
	}

	return retSize
}

var dbpath1 = flag.String("mossPath", "moss-test-data", "path to plasma store directory")
var valuelen = flag.Int("valueLength", 48, "Length of value string.")
var keylen = flag.Int("keyLength", 48, "Length of key string.")
var runtim = flag.String("runTime", "10s", "Test Length")
var samplefreq = flag.String("sampleFrequency", "1s", "Sample Frequency")
var numwriters = flag.Int("numWriters", 1, "number of write threads")
var writebatchsize = flag.Int("writeBatchSize", 10000, "# of keys to insert as a batch; must be > 0")
var writebatchtt = flag.String("writeBatchThinkTime", "0ms", "Think time between write batches")
var keyorder = flag.String("keyOrder", "rand", "desc, asc, rand")
var outputtofile = flag.Bool("outputToFile", false, "Save output to Results_*.json file")
var rundesc = flag.String("runDescription", "-", "Description of run")
var numreaders = flag.Int("numReaders", 1, "number of read threads")
var readbatchsize = flag.Int("readBatchSize", 100, "number of keys to read in range scan; must be > 0")
var readbatchtt = flag.String("readBatchThinkTime", "0ms", "Think time between read batches")
var startkey = flag.Int("startKey", 0, "start key value")
var dbsize = flag.String("dbSize", "0", "Create a database of this size")
var dbcreate = flag.Bool("dbCreate", false, "Create the database")
var diskmon = flag.String("diskMonitor", "sdc", "Device in /proc/diskstats to monitor")
var memquota1 = flag.String("memoryQuota", "4gb", "Memory Quota")
var compperc = flag.Float64("compactionPercentage", 0.65, "Compaction Percentage")
var compmaxsegs = flag.Int("compactionLevelMaxSegments", 9, "CompactionLevelMaxSegments")
var compmult = flag.Int("compactionLevelMultiplier", 3, "CompactionLevelMultiplier")

var CompactionLevelMultiplier int

func (dgm *dgmTest) dgmTestArgs(t *testing.T) {
	flag.Parse()

	dgm.dbPath = *dbpath1

	dgm.dbCreate = *dbcreate
	dgm.keyLength = *keylen
	dgm.keyLengthFmt = fmt.Sprintf("%%0%dd", dgm.keyLength)
	dgm.valueLength = *valuelen
	dgm.numWriters = *numwriters
	dgm.writeBatchSize = (uint64)(*writebatchsize)
	if dgm.writeBatchSize == 0 {
		fmt.Printf("Invalid writeBatchSize (%v)... changing it to 1\n", *writebatchsize)
		dgm.writeBatchSize = 1
	}
	dgm.outputToFile = *outputtofile
	dgm.diskMonitor = *diskmon
	dgm.runDescription = *rundesc
	dgm.numReaders = *numreaders
	dgm.startKey = (uint64)(*startkey)
	dgm.readBatchSize = (uint64)(*readbatchsize)
	if dgm.readBatchSize == 0 {
		fmt.Printf("Invalid readBatchSize (%v)... changing it to 1\n", *readbatchsize)
		dgm.readBatchSize = 1
	}
	dgm.t = t

	switch {
	case strings.Contains(strings.ToLower(*keyorder), "asc"):
		dgm.keyOrder = AscendingKO
	case strings.Contains(strings.ToLower(*keyorder), "rand"):
		dgm.keyOrder = RandomKO
	default:
		panic(fmt.Sprintf("Invalid keyOrder %v\n", *keyorder))
	}

	val, err := time.ParseDuration(*runtim)
	if err != nil {
		t.Fatalf("Invalid runTime %v - %v", *runtim, err)
	}
	dgm.runTime = val

	val, err = time.ParseDuration(*samplefreq)
	if err != nil {
		t.Fatalf("Invalid sampleFrequency %v - %v", *samplefreq, err)
	}
	dgm.sampleFrequency = val

	val, err = time.ParseDuration(*writebatchtt)
	if err != nil {
		t.Fatalf("Invalid writeBatchThinkTime %v - %v", *writebatchtt, err)
	}
	dgm.writeBatchThinkTime = val

	val, err = time.ParseDuration(*readbatchtt)
	if err != nil {
		t.Fatalf("Invalid readBatchThinkTime %v - %v", *readbatchtt, err)
	}
	dgm.readBatchThinkTime = val

	dgm.dbSize = parseSize(*dbsize)
	dgm.memQuota = parseSize(*memquota1)
	if !dgm.dbCreate && dgm.dbSize > 0 {
		dgm.numKeysStart = dgm.dbSize / (uint64)(dgm.keyLength+dgm.valueLength)
		if dgm.startKey == 0 {
			dgm.startKey = dgm.numKeysStart
			fmt.Printf("startKey=%d\n", dgm.startKey)
		}
	}

}

func (dgm *dgmTest) openMossStore() {
	so := DefaultStoreOptions
	so.CollectionOptions.MinMergePercentage = 0.0
	so.CollectionOptions.MergerIdleRunTimeoutMS = 1000
	so.CollectionOptions.OnEvent = NewMossHerderOnEventDGM(dgm)
	so.CompactionPercentage = *compperc
	so.CompactionLevelMaxSegments = *compmaxsegs
	so.CompactionLevelMultiplier = *compmult
	so.CompactionSync = true
	spo := StorePersistOptions{CompactionConcern: CompactionAllow} //, AsyncCompaction: true}

	s, c, e := OpenStoreCollection(dgm.dbPath, so, spo)
	if e != nil || s == nil || c == nil {
		se := fmt.Sprintf("Error OpenStoreCollection(%s) - %v", dgm.dbPath, e)
		panic(se)
	}
	dgm.store = s
	dgm.collection = c
}

func (dgm *dgmTest) closeMossStore() {
	tm := time.Now()
	fmt.Printf("%02d:%02d:%02d - Closing Collections...", tm.Hour(), tm.Minute(), tm.Second())

	waitForPersistence(dgm.collection)
	time.Sleep(1 * time.Second) // wait for idle run compaction to kick in.
	dgm.collection.Close()
	dgm.store.Close()
	fmt.Printf("Done %.3f\n", time.Since(tm).Seconds())
}

func TestMossDGM(t *testing.T) {
	dgm := dgmTest{}
	dgm.dgmTestArgs(t)
	st := time.Now()
	pid := os.Getpid()
	dgm.resultsFileName = fmt.Sprintf("Results_%02d%02d%02d_%d", st.Hour(), st.Minute(), st.Second(), pid)

	if stat, err := os.Stat(dgm.dbPath); err != nil || !stat.IsDir() {
		if err == nil {
			fmt.Println("os.RemoveAll")
			os.RemoveAll(dgm.dbPath)
		}
		if err := os.Mkdir(dgm.dbPath, 0777); err != nil {
			es := fmt.Sprintf("Can't create directory %s: %v", dgm.dbPath, err)
			panic(es)
		}
		defer os.RemoveAll(dgm.dbPath)
	}

	if dgm.dbCreate {
		os.RemoveAll(dgm.dbPath)
		_ = os.Mkdir(dgm.dbPath, 0777)
	}

	dgm.openMossStore()

	lastSampleTime := time.Now()
	firstStats := dgm.getDGMStats(time.Since(lastSampleTime))
	dgm.waitGroupSample.Add(1)
	go dgm.sampleStats(firstStats)
	if dgm.dbCreate && dgm.dbSize > 0 {
		dgm.createItems()
	}

	if dgm.numWriters > 0 || dgm.numReaders > 0 {
		if dgm.numWriters > 0 {
			dgm.waitGroupWriters.Add(dgm.numWriters)
			for w := 0; w < dgm.numWriters; w++ {
				go dgm.addItems(w)
			}
		}

		if dgm.numReaders > 0 {
			dgm.waitGroupReaders.Add(dgm.numReaders)
			for r := 0; r < dgm.numReaders; r++ {
				go dgm.readItems(r)
			}
		}

		time.Sleep(dgm.runTime)

		atomic.StoreInt32(&dgm.workersStop, 1)

		if dgm.numWriters > 0 {
			dgm.waitGroupWriters.Wait()
		}
		if dgm.numReaders > 0 {
			dgm.waitGroupReaders.Wait()
		}

		fmt.Printf("Workers Stop...\n")
	}

	atomic.StoreInt32(&dgm.allStop, 1)
	dgm.waitGroupSample.Wait()
	dgm.closeMossStore()
}
