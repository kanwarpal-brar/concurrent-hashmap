#include "hashmap.h"
#include <cmath>
#include <assert.h>
#include <algorithm>

using namespace std;

simple_record_manager<CASHashmap::table> * CASHashmap::recordmanager = new simple_record_manager<table>(MAX_THREADS);


// allocate and zero data array
void CASHashmap::table::allocateData(int tid, int capacity) {
    assert(capacity > 0);
    // simple contiguous array
    data = new atomic<int>[capacity];
    for (int i = 0; i < capacity; ++i) {
        data[i].store(EMPTY, std::memory_order_relaxed);
    }
}

// constructor
CASHashmap::table::table(table *t, int newCapacity, int numThreads, int tid): old(t->data), oldCapacity(t->capacity), chunksClaimed(0), chunksDone(0), approxInserts(numThreads), approxErase(numThreads) {
    assert(newCapacity > 0);
    assert(numThreads >= 1);
    capacity = max(newCapacity, MINIMUMSIZE);  // enforce a minimum size on expansion, don't want to go to 0 just because there's no keys
    allocateData(tid, capacity);
}

// alt constructor: overrides minimum size
CASHashmap::table::table(int _capacity, int numThreads, int tid): capacity(_capacity), chunksClaimed(0), chunksDone(0), approxInserts(numThreads), approxErase(numThreads) {
    allocateData(tid, _capacity);
}

// destructor
CASHashmap::table::~table() {
    delete[] data;
    delete[] old;
}


/**
 * constructor: initialize the hash table's internals
 *
 * @param _numThreads maximum number of threads that will ever use the hash table (i.e., at least tid+1, where tid is the largest thread ID passed to any function of this class)
 * @param _capacity is the INITIAL size of the hash table (maximum number of elements it can contain WITHOUT expansion)
 */
CASHashmap::CASHashmap(const int _numThreads, const int _capacity)
: numThreads(_numThreads), initCapacity(_capacity) {
    auto guard = recordmanager->getGuard(0);  // dummy tid
    currentTable = new(recordmanager->allocate<table>(0)) table(initCapacity, numThreads);
}

// destructor: clean up any allocated memory, etc.
CASHashmap::~CASHashmap() {
    auto guard = recordmanager->getGuard(0); // dummy tid
    table *t = currentTable;
    if (t) {
        recordmanager->deallocate(0, t);
    }
    // currentTable itself is an atomic pointer, not dynamically allocated, so no delete needed for it.
}

int CASHashmap::probeTolerance(const int tid, table *t) {
    // assumption: guard already held
    return min(PROBE_MAX_TOLERANCE, t->capacity - 1);  // edge case: handle probing in small tables
}

bool CASHashmap::expandAsNeeded(const int tid, table * t, int i) {
    // assumption: guard already held
    helpExpansion(tid, t);  // start off by seeing if there's an expansion to help
    int half = (t->capacity) / 2;
    int approxUsed = t->approxInserts.get();
    if (approxUsed > half) {
        int approxKeys = approxUsed - t->approxErase.get();
        startExpansion(tid, t, approxKeys*SIZEFACTOR);
        return true;
    }
    if (i > PROBE_MAX_TOLERANCE) {
        int exactUsed = t->approxInserts.getAccurate();
        if (exactUsed > half) {
            int exactKeys = exactUsed - t->approxErase.getAccurate();
            startExpansion(tid, t, exactKeys*SIZEFACTOR);
            return true;
        }
    }
    // no need to expand
    return false;  // continue as needed
}

void CASHashmap::helpExpansion(const int tid, table * t) {
    // assumption: guard already held
    int totalOldChunks = ceil(t->oldCapacity / CHUNKSIZE);
    if (totalOldChunks == 0) return;  // nothing to expand, move on
    while (t->chunksClaimed < totalOldChunks) {
        int myChunk = t->chunksClaimed.fetch_add(1);  // claim a chunk, and increment
        if (myChunk < totalOldChunks) {
            // we have a real chunk, let's work on it
            migrate(tid, t, myChunk);  // migrate my chunk
            t->chunksDone.fetch_add(1);  // increment the number of completed chunks
        }
    }  // we have done our job
    // let's wait
    while (t->chunksDone != totalOldChunks) Pause();  // busy wait?
}


void CASHashmap::startExpansion(const int tid, table * t, const int newSize) {
    // auto guard = recordmanager->getGuard(tid); Assumption: guard already held
    if (currentTable == t) {
        // int cap = t->capacity;
        auto mem = recordmanager->allocate<table>(tid);
        table *t_new = new(mem) table(t, newSize, numThreads, tid);
        if  (!currentTable.compare_exchange_strong(t, t_new)) {
            t_new->old = nullptr; // override
            recordmanager->deallocate(tid, t_new);  // failed to cas, delete the table
        } else recordmanager->retire(tid, t);  // retire old table data
        // else TPRINT("Expanding Table at: " << timer.getElapsedMillis() << "ms " << cap << "->" << newSize)
    }
    helpExpansion(tid, currentTable);  // let's help expand now
}


void CASHashmap::migrate(const int tid, table * t, int myChunk) {
    // assumption: guard already held
    int start = myChunk * CHUNKSIZE;
    int end = min(start + (int)CHUNKSIZE, t->oldCapacity);
    assert(start >= 0);
    assert(end <= t->oldCapacity);
    for (int i = start; i < end; i++) {
        int found = t->old[i];  // starting val
        for(;;) {
            if (found == TOMBSTONE) break;  // move on if we find a tombstone
            // retry loop
            if (t->old[i].compare_exchange_strong(found, found | MARKED_MASK)) break;  // successfully marked the value, stop retry
            // failed to CAS, someone did EMPTY->KEY or KEY->TOMBSTONE
            found = t->old[i]; // grab the value at chunk)
        }
        if (found > EMPTY && found < TOMBSTONE) insertIfAbsent(tid, found, true);  // insert with expansion disabled
    }
    // done with migration
}

// try to insert key. return true if successful (if key doesn't already exist), and false otherwise
bool CASHashmap::insertIfAbsent(const int tid, const int & key, bool disableExpansion) {
    assert(key != EMPTY);
    assert(key != TOMBSTONE);

retry:

    auto guard = recordmanager->getGuard(tid);

    table *t = currentTable;

    uint32_t h = murmur3(key);  // grab the hash

    // Start probe insert loop
    assert(t->capacity > 0);
    for (int i = 0; i < t->capacity; ++i) {
        if (!disableExpansion && expandAsNeeded(tid, t, i)) goto retry;  // check (and help) expansion, then retry insert

        assert(h >= 0);
        assert(t->capacity >= 0);
        int index = (h + i) % t->capacity;
        assert(index >= 0);
        assert(index < t->capacity);
        int found = t->data[index]; // grab data in the current table
        assert(!(disableExpansion && (found & MARKED_MASK)));  // make sure we're not double expanding
        if (found & MARKED_MASK) {
            helpExpansion(tid, currentTable);
            goto retry;  // we found a marked bit, expension is ongoing; help out
        }
        else if (found == key) return false;  // found target key, cannot insert
        else if (found == EMPTY) {
            // found an empty slot, try to insert
            int null = EMPTY;  // local copy
            if (t->data[index].compare_exchange_strong(null, key)) {
                t->approxInserts.inc(tid);
                return true;  // CAS success, we inserted
            } else {
                // Failed CAS, someone inserted
                found = t->data[index];  // update found value
                if (found & MARKED_MASK) {
                    helpExpansion(tid, currentTable);
                    goto retry;  // evidence of expansion, let's retry to help
                }
                else if (found == key) return false;  // found the key we were to insert, return false
            }
        }
        // Fallthrough: we failed to find an empty spot or lost our spot. Probe to find next spot
    }
    assert(false);  // this fallthrough should not happen: we should expand; never run out of capacity
}


// try to erase key. return true if successful, and false otherwise
bool CASHashmap::erase(const int tid, const int & key, bool disableExpansion) {
    assert(key != EMPTY);
    assert(key != TOMBSTONE);

retry:

    auto guard = recordmanager->getGuard(tid);

    table *t = currentTable;

    uint32_t h = murmur3(key);

    // start probe and delete loop
    for (int i = 0; i < t->capacity; i++) {
        if (!disableExpansion && expandAsNeeded(tid, t, i / 2)) goto retry;  // help expansion, then erase on new table

        int index = (h + i) % t->capacity;  // grab index
        int found = t->data[index];  // grab current data at point
        if (found & MARKED_MASK) {
            helpExpansion(tid, currentTable);
            goto retry;  // expansion is ongoing, help out then erase after
        }
        if (found == EMPTY) return false;  // we found an empty, this probably means key is not present
        else if (found == key) {
            // found our target, try to CAS
            int k = key;  // local duplicate
            if (t->data[index].compare_exchange_strong(k, TOMBSTONE)) {
                t->approxErase.inc(tid);
                return true;  // successfully removed
            }
            // else: failed CAS, must mean someone removed before us or expansion
            found = t->data[index];
            if (found & MARKED_MASK) {
                helpExpansion(tid, currentTable);
                goto retry;
            }

            return false;  // we lost because someone removed before us, return false
        }
        // Fallthrough: some other key or tombstone, probe forward
    }  // fallthrough: failed to find target
    return false;  // key not present
}

// semantics: return the sum of all KEYS in the set
// Note: This provides a snapshot sum, which might not be consistent in a concurrent environment
int64_t CASHashmap::getSumOfKeys() {
    auto guard = recordmanager->getGuard(0); // dummy id
    table * t = currentTable;
    int64_t sum = 0;
    for (int i = 0; i < t->capacity; i++) {
        int found = t->data[i];
        if (found != EMPTY && found != TOMBSTONE) sum += found;
    }
    return sum;
}
