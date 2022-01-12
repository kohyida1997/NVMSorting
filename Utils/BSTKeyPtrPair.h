#pragma once

#include "Record.h"

struct BSTKeyPtrPair {
    uint64_t key;
    Record* recordPtr;
    BSTKeyPtrPair* left = nullptr;
    BSTKeyPtrPair* right = nullptr;
};