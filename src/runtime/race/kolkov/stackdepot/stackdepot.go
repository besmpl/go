// Package stackdepot implements bounded stack trace storage and exact
// deduplication for race reports.
package stackdepot

import (
	"internal/runtime/atomic"
	"unsafe"
)

const (
	// MaxFrames is the maximum number of stack frames retained for a capture.
	MaxFrames = 8

	// depotSize is the maximum number of unique stacks retained in one
	// generation. Stack IDs use a 1-based record index, so 17 bits are needed
	// to represent every record while reserving ID zero as unavailable.
	depotSize       = 65536
	recordIndexBits = 17
	recordIndexMask = uint64(1<<recordIndexBits) - 1
	// maxGeneration is reserved as a terminal exhausted state. It is never
	// encoded into an issued ID, so generation rollover cannot revive stale IDs.
	maxGeneration = uint64(1<<(64-recordIndexBits)) - 1

	// Hash buckets are bounded by the fixed record arena. Colliding records are
	// linked through immutable record indexes, so lookup does not have an
	// arbitrary probe limit.
	bucketCount = 1024
)

// StackTrace is the report-facing view of a stored trace. Trailing entries are
// zero. The exact frame count is retained in the depot record.
type StackTrace struct {
	PC [MaxFrames]uintptr
}

// depotRecord is initialized completely before ready and its bucket head are
// published. It is never changed again during that generation.
type depotRecord struct {
	trace  StackTrace
	hash   uint64
	next   uint32 // 1-based record index, or zero at the end of the chain.
	frames uint8
	ready  atomic.Uint64 // generation; release-publishes all fields above.
}

type depotBucket struct {
	lock atomic.Uint32
	head atomic.Uint32 // 1-based record index.
}

var (
	stackRecords    [depotSize]depotRecord
	stackBuckets    [bucketCount]depotBucket
	recordCount     atomic.Uint32
	depotGeneration atomic.Uint64
)

// CaptureStack captures the current stack and returns an opaque stack ID.
// Equal stacks in the current generation return the same ID. Zero means that
// no stack was available, the fixed depot is full, or generations are
// exhausted.
func CaptureStack() uint64 {
	var pcs [MaxFrames]uintptr
	// Skip runtime.Callers and CaptureStack. The non-race runtime helper adjusts
	// for its public-API wrapper.
	n := runtimeCallers(2, pcs[:])
	if n == 0 {
		return 0
	}
	return storeStack(pcs[:n], hashStack(pcs[:n]))
}

// storeStack stores pcs using hash for bucket selection. Keeping the hash as
// an input makes collision behavior directly testable; hash is never exposed
// as stack identity.
func storeStack(pcs []uintptr, hash uint64) uint64 {
	if len(pcs) == 0 {
		return 0
	}
	if len(pcs) > MaxFrames {
		pcs = pcs[:MaxFrames]
	}

	generation := currentGeneration()
	if generation == 0 {
		return 0
	}
	bucket := &stackBuckets[hash&(bucketCount-1)]
	lockBucket(bucket)
	defer unlockBucket(bucket)

	for recordIndex := bucket.head.LoadAcquire(); recordIndex != 0; {
		record := &stackRecords[recordIndex-1]
		if record.ready.Load() == generation &&
			record.hash == hash && equalStack(record, pcs) {
			return encodeID(generation, recordIndex)
		}
		recordIndex = record.next
	}

	recordIndex := reserveRecord()
	if recordIndex == 0 {
		return 0
	}

	record := &stackRecords[recordIndex-1]
	record.trace.PC = [MaxFrames]uintptr{}
	copy(record.trace.PC[:], pcs)
	record.hash = hash
	record.frames = uint8(len(pcs))
	record.next = bucket.head.Load()

	// Direct ID lookup synchronizes with ready. Bucket traversal synchronizes
	// with head. Both publications happen only after every immutable field is
	// initialized.
	record.ready.Store(generation)
	bucket.head.StoreRelease(recordIndex)
	return encodeID(generation, recordIndex)
}

func equalStack(record *depotRecord, pcs []uintptr) bool {
	if int(record.frames) != len(pcs) {
		return false
	}
	for i, pc := range pcs {
		if record.trace.PC[i] != pc {
			return false
		}
	}
	return true
}

func reserveRecord() uint32 {
	for {
		count := recordCount.Load()
		if count >= depotSize {
			return 0
		}
		if recordCount.CompareAndSwap(count, count+1) {
			return count + 1
		}
	}
}

func lockBucket(bucket *depotBucket) {
	for {
		if bucket.lock.CompareAndSwap(0, 1) {
			return
		}
		for bucket.lock.LoadAcquire() != 0 {
		}
	}
}

func unlockBucket(bucket *depotBucket) {
	bucket.lock.StoreRelease(0)
}

func currentGeneration() uint64 {
	for {
		generation := depotGeneration.Load()
		if generation == maxGeneration {
			return 0
		}
		if generation != 0 {
			return generation
		}
		if depotGeneration.CompareAndSwap(0, 1) {
			return 1
		}
	}
}

func encodeID(generation uint64, recordIndex uint32) uint64 {
	return generation<<recordIndexBits | uint64(recordIndex)
}

// GetStack retrieves the exact immutable record named by id. It does not
// search by hash. IDs from older generations and malformed IDs are unavailable.
func GetStack(id uint64) *StackTrace {
	if id == 0 {
		return nil
	}
	generation := id >> recordIndexBits
	recordIndex := id & recordIndexMask
	if generation == 0 || generation != depotGeneration.Load() ||
		recordIndex == 0 || recordIndex > depotSize {
		return nil
	}

	record := &stackRecords[recordIndex-1]
	if record.ready.Load() != generation {
		return nil
	}
	return &record.trace
}

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// hashStack computes FNV-1a over the exact frame count and PCs.
//
//go:nosplit
func hashStack(pcs []uintptr) uint64 {
	hash := uint64(fnvOffset64)
	hash ^= uint64(len(pcs))
	hash *= fnvPrime64
	for _, pc := range pcs {
		for shift := uint(0); shift < uint(unsafe.Sizeof(pc))*8; shift += 8 {
			hash ^= uint64(byte(pc >> shift))
			hash *= fnvPrime64
		}
	}
	return hash
}

// FormatStack formats a trace in the form used by race reports and omits
// runtime-internal frames.
func (st *StackTrace) FormatStack() string {
	if st == nil {
		return "  <unknown>\n"
	}

	frameCount := 0
	for frameCount < len(st.PC) && st.PC[frameCount] != 0 {
		frameCount++
	}
	if frameCount == 0 {
		return "  <runtime internal>\n"
	}
	frames := runtimeCallersFrames(st.PC[:frameCount])

	result := ""
	for {
		pc, function, file, line, more := runtimeFramesNext(frames)
		if pc == 0 {
			break
		}
		if hasPrefix(function, "runtime.") {
			if !more {
				break
			}
			continue
		}
		result += "  " + function + "()\n"
		result += "      " + file + ":" + itoa(line) + "\n"
		if !more {
			break
		}
	}
	if result == "" {
		return "  <runtime internal>\n"
	}
	return result
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// Reset starts an empty generation. It is not safe to call concurrently with
// CaptureStack, GetStack, FormatStack, or Stats. Record storage may be reused
// only after the generation has advanced, so every old ID becomes stale first.
// At generation exhaustion, the depot remains unavailable rather than wrapping.
func Reset() {
	generation := depotGeneration.Load()
	if generation == 0 {
		generation = 1
	} else if generation >= maxGeneration-1 {
		generation = maxGeneration
	} else {
		generation++
	}
	depotGeneration.Store(generation)
	for i := range stackBuckets {
		stackBuckets[i].head.Store(0)
		stackBuckets[i].lock.Store(0)
	}
	recordCount.Store(0)
}

// Stats reports occupied records and their fixed-record footprint. It does
// not count unused capacity in the statically allocated arena.
func Stats() (uniqueStacks int, totalMemory int64) {
	uniqueStacks = int(recordCount.Load())
	totalMemory = int64(uniqueStacks) * int64(unsafe.Sizeof(depotRecord{}))
	return uniqueStacks, totalMemory
}
