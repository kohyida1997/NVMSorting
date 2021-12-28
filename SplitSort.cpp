#include <libpmem.h>
#include <iostream>
#include <algorithm>
#include <vector>
#include <iterator>
#include <thread>
#include <random>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <omp.h>

#include "Record.h"

#define PRINT_SAMPLED_KEYS 0
#define PRESORT_DATA_FOR_TESTING 1

#define PRINT_SORTED_KEYS 0
#define CHECK_KEYS_ARE_SORTED 1

using namespace std;

static const char* UNSORTED_FILE_PATH = "/dcpmm/yida/UNSORTED_KEYS";
static unsigned int numThreads;
static unsigned int numSamples;
static unsigned int numPartitions;
static unsigned long numKeysToSort;

Record* mmapUnsortedFile();
void splitSort(Record* recordsBaseAddr);
void systematicParSample(Record* recordsBaseAddr, vector<uint64_t>* sampledKeys);

int main(int argc, char *argv[]) {

    /*

    Usage: <num_keys_to_sort> <num_threads> <num_samples> <num_partitions>

    */

    if (argc != 5) {
        cout << "Num args supplied = " << argc << endl;
        cout << "Usage: <num_threads> <num_samples> <num_partitions>" << endl;
        return 0;
    }

    numKeysToSort = atol(argv[1]);
    numThreads = atoi(argv[2]);
    numSamples = atoi(argv[3]);
    numPartitions = atoi(argv[4]);
    
    cout << "Number of Records to sort: " << numKeysToSort << endl;
    cout << "Number of Threads used: " << numThreads << endl;
    cout << "Number of Samples taken: " << numSamples << endl;
    cout << "Number of Partitions: " << numPartitions << endl;

    Record* recordBaseAddr = mmapUnsortedFile();
#if PRESORT_DATA_FOR_TESTING
    sort(recordBaseAddr, recordBaseAddr + numKeysToSort, [](Record x, Record y) {return x.key < y.key;});
#endif
    splitSort(recordBaseAddr);
    
    

#if PRINT_SORTED_KEYS
    for (int i = 0; i < numKeysToSort; i++) {
        cout << (recordBaseAddr + i)->key << endl;
    }    
#endif

#if CHECK_KEYS_ARE_SORTED
    cout << "Working... Verifying keys are correctly sorted" << endl;
    for (int i = 1; i < numKeysToSort; i++) {
        if ((recordBaseAddr + i)->key < (recordBaseAddr + i - 1)->key) {
            cout << "!!! Critical Failure. Sorting is incorrect !!!\n";
            exit(1);
        }
    }
    cout << "Working... Success, Keys are in sorted ascending order!\n";
#endif

    return 0;

}

Record* mmapUnsortedFile() {
    size_t targetLength = numKeysToSort * sizeof(Record);
	char *pmemBaseAddr;
    size_t mappedLen;
    int isPmem;
    cout << "Working... Mapping NVM file\n";

    /* create a pmem file and memory map it */
    if ((pmemBaseAddr = (char *) pmem_map_file(UNSORTED_FILE_PATH, targetLength, PMEM_FILE_CREATE, 0666, &mappedLen, &isPmem)) == NULL) {
        perror("Failed to map target file to sort");
        exit(1);
    }

    if (!isPmem) {
        cout << "!!! Warning, mapped PMEM File is NOT in the Optane !!!\n";
    }

    return (Record*) pmemBaseAddr;

}

void splitSort(Record* recordsBaseAddr) {

    // Sample records
    vector<uint64_t>* sampledKeys = new vector<uint64_t>();
    systematicParSample(recordsBaseAddr, sampledKeys);

    // Sort samples

    // Create partitions

    // Insert into partitions

    // Read out the partitions

    delete sampledKeys;

}

void systematicParSample(Record* recordsBaseAddr, vector<uint64_t>* sampledKeys) {

    sampledKeys->resize(numKeysToSort);
    int stepSize = numKeysToSort / numSamples;
    
    cout << "Working... Sampling Records (keys only)\n";

    #pragma omp parallel for num_threads(numThreads)
    for (int i = 0; i < numSamples; i++) {
        (*sampledKeys)[i] = (recordsBaseAddr + (i * stepSize))->key;
    }


#if PRINT_SAMPLED_KEYS 
    cout << "Printing... Sampled Keys\n";
    auto temp = *sampledKeys;
    for (int i = 0; i < numSamples; i++) {
        cout << "Sample " << i << ": " << temp[i] << endl;
    }
#endif

}

