package syncshadow

import "internal/runtime/atomic"

const (
	// Sync entries are grouped first by application page and then by cache-line
	// sized address segment. This keeps ordinary allocation/free range clearing
	// proportional to the pages it touches without reserving shadow state for
	// every byte in the application address space.
	syncPageShift       = 12
	syncSegmentShift    = 6
	syncSegmentsPerPage = 1 << (syncPageShift - syncSegmentShift)
	syncPageMask        = uintptr(1<<syncPageShift) - 1

	// Top-level buckets shard writers while readers remain lock-free. A few
	// thousand buckets keep live page chains short without the 1 MiB fixed table
	// (and full-table range scan) used by the old open-address implementation.
	syncBucketCount = 1 << 12
	syncBucketMask  = syncBucketCount - 1

	// For a short range, direct page lookup is cheaper than visiting every
	// bucket. Beyond this point, scanning only pages which actually contain sync
	// state avoids work proportional to a potentially huge freed span.
	syncDirectClearPages = syncBucketCount
)

// spinlock is the package-local writer lock. Importing sync here would create
// a runtime dependency cycle, so writers use the same atomic-only locking
// pattern as the detector and ordinary shadow memory.
type spinlock struct {
	state atomic.Uint32
}

func (l *spinlock) lock() {
	for !l.state.CompareAndSwap(0, 1) {
		// Sync metadata writes are short and allocation-free while locked.
	}
}

func (l *spinlock) unlock() {
	l.state.Store(0)
}

// syncCell owns one address-to-SyncVar association. addr and syncVar are
// immutable after publication. next may only be changed by the owning bucket's
// writer, and is atomic so lock-free readers can safely traverse while a range
// clear unlinks cells.
type syncCell struct {
	addr    uintptr
	syncVar *SyncVar
	next    atomic.Pointer[syncCell]
}

// syncPage contains the live sync entries for one application page. Segmenting
// by 64-byte address regions bounds lookup and partial-clear chains even for a
// dense array of synchronization primitives.
type syncPage struct {
	number uintptr
	next   atomic.Pointer[syncPage]

	segments [syncSegmentsPerPage]atomic.Pointer[syncCell]
	// entryCount is protected by the owning bucket lock. Readers do not inspect
	// it; it exists only so the last range deletion can unlink the empty page.
	entryCount uint32
}

type syncBucket struct {
	mu    spinlock
	pages atomic.Pointer[syncPage]
}

// SyncShadow maps synchronization primitive addresses to their happens-before
// state.
//
// The map is an exact, dynamically sized chained index:
//   - reads and existing-address GetOrCreate calls are lock-free;
//   - misses and deletion are serialized per top-level bucket;
//   - chains are never capacity-limited, so a collision cannot evict live HB;
//   - allocation/free range deletion unlinks stale address owners;
//   - removed nodes remain valid for concurrent readers until Go's GC proves
//     that no reader still holds them.
//
// A writer holds exactly one bucket lock at a time. It never allocates while
// holding that lock, which both defines the lock order and keeps runtime hook
// latency bounded.
type SyncShadow struct {
	buckets [syncBucketCount]syncBucket
}

// NewSyncShadow returns an empty synchronization shadow map.
func NewSyncShadow() *SyncShadow {
	return &SyncShadow{}
}

// fastHashSync returns the top-level bucket for addr. The page number is mixed
// rather than the byte offset because all entries on one page share a page
// record.
//
//go:nosplit
func fastHashSync(addr uintptr) uint64 {
	x := uint64(addr >> syncPageShift)
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x & syncBucketMask
}

// segmentIndex returns the 64-byte segment containing addr within its page.
//
//go:nosplit
func segmentIndex(addr uintptr) uintptr {
	return (addr >> syncSegmentShift) & (syncSegmentsPerPage - 1)
}

// findPage returns the current page record from a bucket snapshot.
//
//go:nosplit
func findPage(bucket *syncBucket, number uintptr) *syncPage {
	for page := bucket.pages.Load(); page != nil; page = page.next.Load() {
		if page.number == number {
			return page
		}
	}
	return nil
}

// findSyncVar returns the current address owner in page.
//
//go:nosplit
func findSyncVar(page *syncPage, addr uintptr) *SyncVar {
	for cell := page.segments[segmentIndex(addr)].Load(); cell != nil; cell = cell.next.Load() {
		if cell.addr == addr {
			return cell.syncVar
		}
	}
	return nil
}

// GetOrCreate returns the unique SyncVar currently owned by addr, creating it
// on first use. Existing-address lookups perform only atomic loads and never
// allocate. A miss allocates candidates before taking the bucket writer lock,
// then rechecks ownership so concurrent creators cannot publish two identities.
func (s *SyncShadow) GetOrCreate(addr uintptr) *SyncVar {
	pageNumber := addr >> syncPageShift
	bucket := &s.buckets[fastHashSync(addr)]
	if page := findPage(bucket, pageNumber); page != nil {
		if syncVar := findSyncVar(page, addr); syncVar != nil {
			return syncVar
		}
	}

	// Allocate outside the writer critical section. Losing a concurrent first-
	// creator race may discard these candidates, but every lookup after the
	// first publication remains allocation-free.
	syncVar := new(SyncVar)
	cell := &syncCell{addr: addr, syncVar: syncVar}
	var newPage *syncPage

	for {
		bucket.mu.lock()
		page := findPage(bucket, pageNumber)
		if page != nil {
			if existing := findSyncVar(page, addr); existing != nil {
				bucket.mu.unlock()
				return existing
			}

			segment := &page.segments[segmentIndex(addr)]
			cell.next.Store(segment.Load())
			segment.Store(cell)
			page.entryCount++
			bucket.mu.unlock()
			return syncVar
		}

		if newPage != nil {
			newPage.segments[segmentIndex(addr)].Store(cell)
			newPage.entryCount = 1
			newPage.next.Store(bucket.pages.Load())
			bucket.pages.Store(newPage)
			bucket.mu.unlock()
			return syncVar
		}
		bucket.mu.unlock()

		// A page is substantially larger than a cell, so allocate it only after
		// the locked recheck proves that this address needs a new page record.
		newPage = &syncPage{number: pageNumber}
	}
}

// HasEntry reports whether addr currently owns synchronization shadow state.
// It is used to suppress reports on a synchronization primitive's own runtime
// fields. ClearRange removes that suppression when allocator lifecycle ends.
//
//go:nosplit
func (s *SyncShadow) HasEntry(addr uintptr) bool {
	bucket := &s.buckets[fastHashSync(addr)]
	page := findPage(bucket, addr>>syncPageShift)
	return page != nil && findSyncVar(page, addr) != nil
}

// clearPageLocked removes entries in the inclusive address range [first,last]
// from page. The caller holds the page's bucket lock.
func clearPageLocked(page *syncPage, first, last uintptr) {
	pageBase := page.number << syncPageShift
	if first == pageBase && last-pageBase == syncPageMask {
		for i := range page.segments {
			page.segments[i].Store(nil)
		}
		page.entryCount = 0
		return
	}

	firstSegment := segmentIndex(first)
	lastSegment := segmentIndex(last)
	for i := firstSegment; i <= lastSegment; i++ {
		segment := &page.segments[i]
		var previous *syncCell
		for cell := segment.Load(); cell != nil; {
			next := cell.next.Load()
			if cell.addr >= first && cell.addr <= last {
				if previous == nil {
					segment.Store(next)
				} else {
					previous.next.Store(next)
				}
				page.entryCount--
			} else {
				previous = cell
			}
			cell = next
		}
	}
}

// clearPage removes a range intersection from one application page.
func (s *SyncShadow) clearPage(pageNumber, first, last uintptr) {
	bucket := &s.buckets[fastHashSync(pageNumber<<syncPageShift)]
	if findPage(bucket, pageNumber) == nil {
		return
	}

	bucket.mu.lock()
	var previous *syncPage
	for page := bucket.pages.Load(); page != nil; {
		next := page.next.Load()
		if page.number != pageNumber {
			previous = page
			page = next
			continue
		}

		clearPageLocked(page, first, last)
		if page.entryCount == 0 {
			if previous == nil {
				bucket.pages.Store(next)
			} else {
				previous.next.Store(next)
			}
		}
		break
	}
	bucket.mu.unlock()
}

// clearAllPagesInRange visits only page records which currently own sync
// state. It is used for spans larger than the top-level directory.
func (s *SyncShadow) clearAllPagesInRange(firstPage, lastPage, first, last uintptr) {
	for i := range s.buckets {
		bucket := &s.buckets[i]
		if bucket.pages.Load() == nil {
			continue
		}

		bucket.mu.lock()
		var previous *syncPage
		for page := bucket.pages.Load(); page != nil; {
			next := page.next.Load()
			if page.number < firstPage || page.number > lastPage {
				previous = page
				page = next
				continue
			}

			pageFirst := page.number << syncPageShift
			pageLast := pageFirst | syncPageMask
			if pageFirst < first {
				pageFirst = first
			}
			if pageLast > last {
				pageLast = last
			}
			clearPageLocked(page, pageFirst, pageLast)
			if page.entryCount == 0 {
				if previous == nil {
					bucket.pages.Store(next)
				} else {
					previous.next.Store(next)
				}
			} else {
				previous = page
			}
			page = next
		}
		bucket.mu.unlock()
	}
}

// ClearRange removes synchronization shadow entries whose addresses are in
// [addr, addr+size). It is called on allocation and free so an address reused by
// a different object cannot inherit happens-before state or report suppression.
//
// Short ranges perform one indexed lookup per covered page. Very large ranges
// scan only live page records, making work proportional to actual sync state
// rather than the byte size of a large freed span.
func (s *SyncShadow) ClearRange(addr, size uintptr) {
	if size == 0 || size-1 > ^uintptr(0)-addr {
		return
	}

	last := addr + size - 1
	firstPage := addr >> syncPageShift
	lastPage := last >> syncPageShift
	pageCount := lastPage - firstPage + 1
	if pageCount > syncDirectClearPages {
		s.clearAllPagesInRange(firstPage, lastPage, addr, last)
		return
	}

	for pageNumber := firstPage; ; pageNumber++ {
		first := pageNumber << syncPageShift
		pageLast := first | syncPageMask
		if first < addr {
			first = addr
		}
		if pageLast > last {
			pageLast = last
		}
		s.clearPage(pageNumber, first, pageLast)
		if pageNumber == lastPage {
			return
		}
	}
}

// Reset clears all synchronization state. Reset is intended for detector test
// setup and teardown; callers must not use it concurrently with detector work.
func (s *SyncShadow) Reset() {
	for i := range s.buckets {
		bucket := &s.buckets[i]
		bucket.mu.lock()
		bucket.pages.Store(nil)
		bucket.mu.unlock()
	}
}
