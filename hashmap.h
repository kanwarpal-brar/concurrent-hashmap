#pragma once
#include "util.h"
#include <atomic>
#include <cmath>
#include <assert.h>
using namespace std;

#define Pause() __asm__ __volatile__("pause" :::) // efficient busy-wait (Intel dark secret)

// Configs
#define MINIMUMSIZE 1 // minimum table size
#define CHUNKSIZE 4096.0  // how big are chunks

// NOTE: through experimentation, I arrived at 5-45.
#define PROBE_MAX_TOLERANCE 45  // at most, we will allow probing to 10 before expansion
#define SIZEFACTOR 5  // how large of a size to expand to

class Hashmap {
private:
    enum {
        MARKED_MASK = (int) 0x80000000,     // most significant bit of a 32-bit key
        TOMBSTONE = (int) 0x7FFFFFFF,       // largest value that doesn't use bit MARKED_MASK
        EMPTY = (int) 0
    }; // with these definitions, the largest "real" key we allow in the table is 0x7FFFFFFE, and the smallest is 1

    struct alignas(64) table {
        char padding0[PADDING_BYTES];  // pre-padding
        atomic<int> *data = nullptr;  // latest data array
        char padding1[PADDING_BYTES];
        atomic<int> *old = nullptr;  // previous data array
        char padding4[PADDING_BYTES];
        int capacity = 0;  // capacity of data array
        int oldCapacity = 0;  // capacity of old array
        counter approxInserts;
        char padding2[2*PADDING_BYTES];
        counter approxErase;
        char padding3[2*PADDING_BYTES];
        atomic<int> chunksClaimed = 0;  // during expansion, the chunk numbers claimed so far [0, ceil(capacity/4096))
        atomic<int> chunksDone = 0;  // during expansion, number of chunks (of total) finished so far, stop at capacity/4096
        char padding5[2*PADDING_BYTES];
        
        // allocate and zero data array
        void allocateData(int capacity) {
            assert(capacity > 0);
            data = new atomic<int>[capacity]{0};
        }

        // constructor
        table(table *t, int newCapacity = -1, int numThreads = 1): old(t->data), oldCapacity(t->capacity), chunksClaimed(0), chunksDone(0), approxInserts(numThreads), approxErase(numThreads) {
            assert(newCapacity > 0);
            assert(numThreads >= 1);
            capacity = max(newCapacity, MINIMUMSIZE);  // enforce a minimum size on expansion, don't want to go to 0 just because there's no keys 
            allocateData(capacity);
        }

        // alt constructor: overrides minimum size
        table(int _capacity, int numThreads = 1): capacity(_capacity), chunksClaimed(0), chunksDone(0), approxInserts(numThreads), approxErase(numThreads) {
            allocateData(_capacity);
        }
        
        // destructor
        ~table() {
            delete[] data;
        }
    };
    
    
    bool expandAsNeeded(const int tid, table * t, int i);
    void helpExpansion(const int tid, table * t);
    void startExpansion(const int tid, table * t, const int newSize);
    void migrate(const int tid, table * t, int myChunk);

    int probeTolerance(const int tid, table *t);

    char padding0[PADDING_BYTES];
    int numThreads;
    int initCapacity;
    char padding1[PADDING_BYTES];
    atomic<table *> currentTable;  // single source of truth for table
    char padding2[PADDING_BYTES];
    
public:
    Hashmap(const int _numThreads, const int _capacity);
    ~Hashmap();
    bool insertIfAbsent(const int tid, const int & key, bool disableExpansion);
    bool erase(const int tid, const int & key, bool disableExpansion);
    int64_t getSumOfKeys();
    void printDebuggingDetails(); 
};

/**
 * constructor: initialize the hash table's internals
 * 
 * @param _numThreads maximum number of threads that will ever use the hash table (i.e., at least tid+1, where tid is the largest thread ID passed to any function of this class)
 * @param _capacity is the INITIAL size of the hash table (maximum number of elements it can contain WITHOUT expansion)
 */
Hashmap::Hashmap(const int _numThreads, const int _capacity)
: numThreads(_numThreads), initCapacity(_capacity), currentTable(new table(initCapacity, numThreads)) { }

// destructor: clean up any allocated memory, etc.
Hashmap::~Hashmap() {
     // we're done, do some cleanup
    table *t = currentTable;
    delete[] t->old;
    delete currentTable;
}

int Hashmap::probeTolerance(const int tid, table *t) {
    return min(PROBE_MAX_TOLERANCE, t->capacity - 1);  // edge case: handle probing in small tables
}

bool Hashmap::expandAsNeeded(const int tid, table * t, int i) {
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

void Hashmap::helpExpansion(const int tid, table * t) {
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

void Hashmap::startExpansion(const int tid, table * t, const int newSize) {
    if (currentTable == t) {
        // int cap = t->capacity;
        table *t_new = new table(t, newSize, numThreads);
        if  (!currentTable.compare_exchange_strong(t, t_new)) {
            delete t_new;  // failed to cas, delete the table
        }
        // success, retire old table
        // else TPRINT("Expanding Table at: " << timer.getElapsedMillis() << "ms " << cap << "->" << newSize)
    }
    helpExpansion(tid, currentTable);  // let's help expand now
}

void Hashmap::migrate(const int tid, table * t, int myChunk) {
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
bool Hashmap::insertIfAbsent(const int tid, const int & key, bool disableExpansion = false) {
    assert(key != EMPTY);
    assert(key != TOMBSTONE);

    table *t = currentTable;

    uint32_t h = murmur3(key);  // grab the hash

    // Start probe insert loop
    assert(t->capacity > 0);
    for (int i = 0; i < t->capacity; ++i) {
        if (!disableExpansion && expandAsNeeded(tid, t, i)) return insertIfAbsent(tid, key);  // check (and help) expansion, then retry insert

        assert(h >= 0);
        assert(t->capacity >= 0);
        int index = (h + i) % t->capacity;
        assert(index >= 0);
        assert(index < t->capacity);
        int found = t->data[index]; // grab data in the current table
        assert(!(disableExpansion && (found & MARKED_MASK)));  // make sure we're not double expanding
        if (found & MARKED_MASK) return insertIfAbsent(tid, key);  // we found a marked bit, expension is ongoing; help out
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
                if (found & MARKED_MASK) return insertIfAbsent(tid, key);  // evidence of expansion, let's retry to help
                else if (found == key) return false;  // found the key we were to insert, return false
            }
        }
        // Fallthrough: we failed to find an empty spot or lost our spot. Probe to find next spot
    }
    assert(false);  // this fallthrough should not happen: we should expand; never run out of capacity
}

// try to erase key. return true if successful, and false otherwise
bool Hashmap::erase(const int tid, const int & key, bool disableExpansion = false) {
    assert(key != EMPTY);
    assert(key != TOMBSTONE);
    table *t = currentTable;

    uint32_t h = murmur3(key);

    // start probe and delete loop
    for (int i = 0; i < t->capacity; i++) {
        if (!disableExpansion && expandAsNeeded(tid, t, i / 2)) return erase(tid, key);  // help expansion, then erase on new table

        int index = (h + i) % t->capacity;  // grab index
        int found = t->data[index];  // grab current data at point
        if (found & MARKED_MASK) {
            return erase(tid, key);  // expansion is ongoing, help out then erase after
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
                return erase(tid, key);
            }

            return false;  // we lost because someone removed before us, return false
        }
        // Fallthrough: some other key or tombstone, probe forward
    }  // fallthrough: failed to find target
    return false;  // key not present
}   

// semantics: return the sum of all KEYS in the set
int64_t Hashmap::getSumOfKeys() {
    table * t = currentTable;
    int64_t sum = 0;
    for (int i = 0; i < t->capacity; i++) {
        int found = t->data[i];
        if (found != EMPTY && found != TOMBSTONE) sum += found;
    }
    return sum;
}


void Hashmap::printDebuggingDetails() {
    // const int COLCOUNT = 200;
    // table *t = currentTable;
    // for (int i = 0; i < t->capacity; ++i) {
    //     int found = t->data[i];
    //     assert(!(found & MARKED_MASK));
    //     cout << (found == EMPTY ? "." : (found == TOMBSTONE ? "O" : "X"));
    //     if (i % COLCOUNT == 0) cout << endl;
    // }
    // cout << endl;
}
