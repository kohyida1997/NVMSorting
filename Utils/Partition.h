#pragma once

#include <mutex>
#include <stack>

/* Struct to store partition metadata associated with each BST. (Recall that each partition is one unbalanced BST) */

struct Partition {

    /* We only need to store the lower range of this partition. */
    uint64_t minKey;
    //size_t totalNumNodes = 0;
    size_t currPoolNodes = 0;
    std::mutex mutex;
    std::stack<char* > poolPtrs; // We keep a stack of all the pointers to the separately allocated regions, so that we can unmmap/cleanup after.
    char* currPoolBaseAddr; // current working NVM pool
    BSTKeyPtrPair* rootOfBST = nullptr;
};
