// hashmap.h
#pragma once
#include <atomic>
#include <cmath>
#include "util.h"
#include "external/recordmgr/record_manager.h"
using namespace std;

#define Pause() __asm__ __volatile__("pause" :::) // efficient busy-wait (Intel dark secret)

// Configs
#define MINIMUMSIZE 1 // minimum table size
#define CHUNKSIZE 4096.0  // how big are chunks

// NOTE: through experimentation, I arrived at 5-45.
#define PROBE_MAX_TOLERANCE 45  // at most, we will allow probing to 10 before expansion
#define SIZEFACTOR 5  // how large of a size to expand to

class CASHashmap {
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
        counter * approxInserts = nullptr;
        char padding2[2*PADDING_BYTES];
        counter * approxErase = nullptr;
        char padding3[2*PADDING_BYTES];
        atomic<int> chunksClaimed = 0;  // during expansion, the chunk numbers claimed so far [0, ceil(capacity/4096))
        atomic<int> chunksDone = 0;  // during expansion, number of chunks (of total) finished so far, stop at capacity/4096
        char padding5[2*PADDING_BYTES];

        // allocate and zero data array
        void allocateData(int tid, int capacity);

        // constructor
        table(table *t, int newCapacity = -1, int numThreads = 1, int tid = 0);

        // alt constructor: overrides minimum size
        table(int _capacity, int numThreads = 1, int tid = 0);

        // destructor
        ~table();
    };

    static simple_record_manager<table> * recordmanager;

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
    CASHashmap(const int _numThreads, const int _capacity);
    ~CASHashmap();
    bool insertIfAbsent(const int tid, const int & key, bool disableExpansion = false);
    bool erase(const int tid, const int & key, bool disableExpansion = false);
    int64_t getSumOfKeys();
};
