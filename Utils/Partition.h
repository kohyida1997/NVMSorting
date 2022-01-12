#pragma once

#include <mutex>
#include <stack>

struct Partition {

    /* We only need to store the lower range of this partition. */
    uint64_t minKey;
    std::mutex mutex;
    std::stack<char* > poolPtrs;
    char* currPoolBaseAddr;

};
