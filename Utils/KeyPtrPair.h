#pragma once

#include "Record.h"

struct KeyPtrPair {
    uint64_t key;
    Record* recordPtr;
}