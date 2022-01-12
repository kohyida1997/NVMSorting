#pragma once

#include <libpmem.h>
#include <iostream>
#include <algorithm>
#include <vector>
#include <thread>

#define DEBUG_INFO 0


template <typename T>
T* allocateNVMRegion(size_t targetLength, const char* TARGET_FILE_PATH) {
	char *pmemBaseAddr;
    size_t mappedLen;
    int isPmem;
#if DEBUG_INFO
    std::cout << "Working... Allocating NVM file: " << TARGET_FILE_PATH << "\n";
#endif
    /* create a pmem file and memory map it */
    if ((pmemBaseAddr = (char *) pmem_map_file(TARGET_FILE_PATH, targetLength, PMEM_FILE_CREATE, 0666, &mappedLen, &isPmem)) == NULL) {
        perror("pmem_map_file failed to create NVM region");
        exit(1);
    }

    if (!isPmem) {
        std::cout << "!!! Warning, allocated PMEM File is NOT in the Optane !!!\n";
    }

    if (mappedLen != targetLength) {
        std::cout << "!!! Warning, " << targetLength << " bytes requested by only " << mappedLen << " bytes mapped !!!\n";
    }

    T* tBaseAddr = (T*) pmemBaseAddr;

    return tBaseAddr;
}