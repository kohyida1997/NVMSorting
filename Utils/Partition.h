#pragma once

#include <mutex>
#include <stack>

struct Partition {

    uint64_t minKey;
    std::mutex mutex;
    std::stack<char* > poolPtrs;
    char* currPoolBaseAddr;

};
