#pragma once

#include <mutex>
#include <stack>

/* Struct to store partition metadata associated with each BST. (Recall that each partition is one unbalanced BST) */

struct Partition {

    /* We only need to store the lower range of this partition. */
    uint64_t minKey;
    size_t totalNumNodes = 0;
    size_t currPoolNodes = 0;
    std::mutex mutex;
    std::stack<char* > poolPtrs;
    char* currPoolBaseAddr; // current working NVM pool

};
