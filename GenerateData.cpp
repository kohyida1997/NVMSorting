#include <libpmem.h>
#include <iostream>
#include <algorithm>
#include <vector>
#include <thread>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <omp.h>

#include "Utils/Record.h"
#include "Utils/HelperFunctions.h"

using namespace std;


#define PRINT_GENERATED_KEYS 0
#define CHECK_KEYS 0

static const char* GENERATED_FILE_PATH = "/dcpmm/yida/UNSORTED_KEYS";

int main(int argc, char *argv[]) {

    
    /*

    Usage: <number_of_keys_to_generate> <integer_seed>

    */

   auto numThreads = thread::hardware_concurrency();

    omp_set_dynamic(0);     // Explicitly disable dynamic teams
    omp_set_num_threads(numThreads);     


    if (argc != 3) {
        cout << "Num args supplied = " << argc << endl;
        cout << "Usage: <number_of_keys_to_generate> <integer_seed>" << endl;
        return 0;
    }


    long numKeys = atol(argv[1]);
    long seed = atol(argv[2]);
    srand(seed);

    cout << "Generating Data to Sort" << endl;
    cout << "Record Unit Size = " << sizeof(Record) << " bytes\n";
    cout << "Number of keys to generate: " << numKeys << endl;
    cout << "Using seed: " << seed << endl;
    cout << "Hardware concurrency: " << numThreads << endl;

    vector<uint64_t> keys(numKeys);

    cout << "Working... Creating Keys in DRAM\n";

    #pragma omp parallel for num_threads(numThreads)
    for (int i = 0; i < numKeys; i++) {
        keys[i] = i;
    }

    cout << "Working... Shuffling Keys in DRAM\n";

    random_shuffle(keys.begin(), keys.end());

#if PRINT_GENERATED_KEYS
    for (int i = 0; i < numKeys; i++) {
        cout << keys[i] << endl;
    }
#endif

    size_t targetLength = numKeys * sizeof(Record);

    Record* recordBaseAddr = allocateNVMRegion<Record>(targetLength, GENERATED_FILE_PATH);//(Record*) pmemBaseAddr;

    size_t mappedLen = targetLength;

    cout << "Working... Copying generated keys into NVM\n";

    #pragma omp parallel for num_threads(numThreads)
    for (int i = 0; i < numKeys; i++) {
        Record r;
        BYTE_24 val;
        r.key = keys[i];
        r.value.val[0] = keys[i];
        pmem_memcpy_nodrain((void*) (recordBaseAddr + i), (void*) &r, sizeof(Record));
    }

#if CHECK_KEYS
    cout << "Working... Verifying copying of keys into NVM\n";
    //#pragma omp parallel for num_threads(numThreads)
    for (int i = 0; i < numKeys; i++) {
        if (keys[i] != (recordBaseAddr + i)->key) {
            cout << "Terminating... DRAM Generated keys do not match NVM keys\n";
            return 0;
        }
    }
    cout << "Working... Success, DRAM Generated keys match NVM keys!\n";
#endif

    cout << "Working... Unmapping NVM from address space\n";

    pmem_unmap((char* ) recordBaseAddr, mappedLen);

    cout << "Working... Done!\n";

    cout << "Total size of Records generated (KB) = " << ((double) mappedLen / (1 << 10)) << " KB\n";
    cout << "Total size of Records generated (MB) = " << ((double) mappedLen / (1 << 20)) << " MB\n";
    cout << "Total size of Records generated (GB) = " << ((double) mappedLen / (1 << 30)) << " GB\n";

    return 0;

}