#include <libpmem.h>
#include <iostream>
#include <algorithm>
#include <vector>
#include <iterator>
#include <thread>
#include <random>
#include <string>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <omp.h>

#include "Utils/BSTKeyPtrPair.h"
#include "Utils/KeyPtrPair.h"
#include "Utils/Partition.h"
#include "Utils/Record.h"
#include "Utils/HelperFunctions.h"

#define PRINT_SAMPLED_KEYS 0
#define PRINT_SORTED_SAMPLED_KEYS 0
#define PRINT_UNSORTED_KEYS 0
#define PRINT_PARTITION_INFO 0

#define PRINT_DURING_INORDER_TRAVERSAL 0

#define PRESORT_DATA_FOR_TESTING 0
#define CHECK_KEYS_ARE_SORTED 1

#define RECORD_STATS 0

using namespace std;

static const char* UNSORTED_FILE_PATH = "/dcpmm/yida/UNSORTED_KEYS";
static const char* PARTITION_FILE_PATH_PREFIX = "/dcpmm/yida/PARTITION";
static unsigned int numThreads;

static unsigned int numSamples;

static unsigned int numPartitions;

static KeyPtrPair* finalSortedPairs;

/* 

    ===== NOTE ON MEMORY ALLOCATION INTO PARTITIONS =====

    It is not known at compile time the number of records that will hash into each partition. 

    In the worst case, ALL records may be hashed to a single partition. This is highly unlikely.
    As such, we find the expected (average) number of records that will be hashed into each
    partition. We then dynamically allocate more memory in NVM should we need more.

    Every allocation request for memory in each partition happens in fixed sizes. The amount
    of memory allocated each time is equal to [EXPECTED_NODES_PER_PARTITION * PARTITION_UNIT_FACTOR] 
    nodes mutlipled by the sizeof(BSTKeyPtrPair). This is because we don't insert records directly
    into our partitions, but only key pointer pairs.

*/

static unsigned long expectedNodesPerPartition;
static double partitionUnitFactor = 1.25;
static unsigned long nodesPerAllocation;

static unsigned long numKeysToSort;

Record* mmapUnsortedFile();
void splitSort(Record* recordsBaseAddr);
void systematicParSample(Record* recordsBaseAddr, vector<KeyPtrPair>* sampledKeys);
void stdSortSamples(vector<KeyPtrPair>* sampledKeys);
void parPartitionSamples(vector<KeyPtrPair>* sampledKeys, Partition *partitions);
void processSampleRange(int begin, int end, int index, Partition *partitions, vector<KeyPtrPair>* sampledKeys);
int binSearchPartitionToInsertInto(uint64_t candidateKey, Partition *sortedPartitions);
void insertAllRecordsIntoPartitions(Record* recordsBaseAddr, Partition *partitions);
void insertBSTNode(uint64_t keyToInsert, Record* recordPtr, Partition *targetPartition, int targetPartitionIdx);
int inOrderTraversal(BSTKeyPtrPair* root, int startDisplacement);


int main(int argc, char *argv[]) {

    /*

    Usage: <num_keys_to_sort> <num_threads> <num_samples> <num_partitions>

    */

    omp_set_dynamic(0); // Explicitly disable dynamic teams
    omp_set_num_threads(numThreads);

    if (argc != 5) {
        cout << "Num args supplied = " << argc << endl;
        cout << "Usage: <num_threads> <num_samples> <num_partitions>" << endl;
        return 0;
    }

    numKeysToSort = atol(argv[1]);
    numThreads = atoi(argv[2]);
    numSamples = atoi(argv[3]);
    numPartitions = atoi(argv[4]);
    
    expectedNodesPerPartition = numKeysToSort / numPartitions;
    nodesPerAllocation = expectedNodesPerPartition * partitionUnitFactor;

    cout << "Number of Records to sort: " << numKeysToSort << endl;
    cout << "Number of Threads used: " << numThreads << endl;
    cout << "Number of Samples taken: " << numSamples << endl;
    cout << "Number of Partitions: " << numPartitions << endl;

    Record* recordBaseAddr = mmapUnsortedFile();

    finalSortedPairs = new KeyPtrPair[numKeysToSort];

#if PRINT_UNSORTED_KEYS
    for (int i = 0; i < numKeysToSort; i++) {
        cout << (recordBaseAddr + i)->key << endl;
    }    
#endif

#if PRESORT_DATA_FOR_TESTING
    sort(recordBaseAddr, recordBaseAddr + numKeysToSort, [](Record x, Record y) {return x.key < y.key;});
#endif
    splitSort(recordBaseAddr); 

#if CHECK_KEYS_ARE_SORTED
    cout << "Working... Verifying keys are correctly sorted" << endl;

    int errorRegister = 0;

    #pragma omp parallel for num_threads(64) 
    for (int i = 1; i < numKeysToSort; i++) {
        if ((finalSortedPairs + i)->key < (finalSortedPairs + i - 1)->key || errorRegister != 0) {
            cout << "!!! Critical Failure. Sorting is incorrect !!!\n";
            errorRegister++;
            //exit(1);
        }
    }

    if (errorRegister > 0) {
        cout << "!!! Critical Failure. Sorting is WRONG !!!\n";
        exit(1);
    }

    cout << "Working... Success, Keys are in sorted ascending order! ✓ \n";
#endif

    delete[] finalSortedPairs;

    return 0;

}

void splitSort(Record* recordsBaseAddr) {

    // Sample records
    vector<KeyPtrPair>* sampledKeys = new vector<KeyPtrPair>();
    systematicParSample(recordsBaseAddr, sampledKeys);
    // Sort samples
    stdSortSamples(sampledKeys);

    // Create and initialize partitions
    /* Note: Partition (meta)data to be stored in DRAM. */
    Partition *partitions = new Partition[numPartitions];
    parPartitionSamples(sampledKeys, partitions);

    // Insert into partitions
    insertAllRecordsIntoPartitions(recordsBaseAddr, partitions);

    // Read out the partitions

    // Sub-task: Compute prefix sums sequentially.
    long rollingSum = 0;
    vector<long> startDisplacement(numPartitions);
    startDisplacement[0] = 0;
    rollingSum += partitions[0].currPoolNodes;
    for (int i = 1; i < numPartitions; i++) {
        startDisplacement[i] = rollingSum;
        rollingSum += partitions[i].currPoolNodes;
    }

    #pragma omp parallel for num_threads(numThreads)
    for (int i = 0; i < numPartitions; i++)
        inOrderTraversal(partitions[i].rootOfBST, startDisplacement[i]);

    delete sampledKeys;
    delete[] partitions;

}

void systematicParSample(Record* recordsBaseAddr, vector<KeyPtrPair>* sampledKeys) {

    sampledKeys->resize(numKeysToSort);
    int stepSize = numKeysToSort / numSamples;
    
    cout << "Working... Sampling Records (keys only)\n";

    #pragma omp parallel for num_threads(numThreads)
    for (int i = 0; i < numSamples; i++) {
        (*sampledKeys)[i].key = (recordsBaseAddr + (i * stepSize))->key;
        (*sampledKeys)[i].recordPtr = (recordsBaseAddr + (i * stepSize));
    }

#if PRINT_SAMPLED_KEYS 
    cout << "Printing... Sampled Keys\n";
    auto temp = *sampledKeys;
    for (int i = 0; i < numSamples; i++) {
        cout << "Sample " << i << ": " << temp[i].key << endl;
    }
#endif

}

void stdSortSamples(vector<KeyPtrPair>* sampledKeys) {
    KeyPtrPair* start = &(*sampledKeys)[0];
    sort(start, start + numSamples, [](KeyPtrPair x, KeyPtrPair y) {return x.key < y.key;});

#if PRINT_SORTED_SAMPLED_KEYS 
    cout << "Printing... Sorted Sampled Keys\n";
    auto temp = *sampledKeys;
    for (int i = 0; i < numSamples; i++) {
        cout << "Sample " << i << ": " << (*sampledKeys)[i].key << endl;
    }
#endif

}

void parPartitionSamples(vector<KeyPtrPair>* sampledKeys, Partition *partitions) {

    size_t subVecLen = numSamples / numPartitions;
    size_t subVecLenPlusOne = subVecLen + 1;
    size_t leftOver = numSamples % numPartitions;

    #pragma omp parallel for num_threads(numThreads) 
    for (int i = 0; i < numPartitions; i++) {
        int begin, end;
        if (i < leftOver) {
            begin = i * (subVecLenPlusOne);
            end = begin + (subVecLenPlusOne);
            processSampleRange(begin, end, i, partitions, sampledKeys);
        } else {
            begin = leftOver * (subVecLenPlusOne) + (i - leftOver) * subVecLen;
            end = begin + subVecLen;
            processSampleRange(begin, end, i, partitions, sampledKeys);
        }
    }

}

void processSampleRange(int begin, int end, int index, Partition *partitions, vector<KeyPtrPair>* sampledKeys) {

    Partition* targetPartition = partitions + index;
    targetPartition->minKey = (*sampledKeys)[begin].key;
    KeyPtrPair middleElem = (*sampledKeys)[(begin + end - 1) / 2];
    BSTKeyPtrPair root;
    root.key = middleElem.key;
    root.recordPtr = middleElem.recordPtr;
    root.left = nullptr;
    root.right = nullptr;

    // Create the BST in NVM (or DRAM) with a certain INIT_BST_SIZE
    string partitionNameString(PARTITION_FILE_PATH_PREFIX);
    partitionNameString.append(to_string(index) + "_" + to_string(0));
    BSTKeyPtrPair* partitionBaseAddr = allocateNVMRegion<BSTKeyPtrPair>(nodesPerAllocation * sizeof(BSTKeyPtrPair), partitionNameString.c_str());

    // Insert the middle element as ROOT
    pmem_memcpy_nodrain((void*) partitionBaseAddr, (void*) &root, sizeof(BSTKeyPtrPair));
    targetPartition->rootOfBST = partitionBaseAddr;

    //targetPartition->totalNumNodes = 1;
    targetPartition->currPoolNodes = 1;
    targetPartition->currPoolBaseAddr = (char*) partitionBaseAddr;
    targetPartition->poolPtrs.push((char*) partitionBaseAddr);

#if PRINT_PARTITION_INFO
    cout << "Partition " << index << ": " << (end - begin) << " elements. [" << begin << ", " << end << "] Root key = " << partitionBaseAddr->key << "\n";
#endif
}

/* Returns the index of the correct Partition to insert into. (non-recursive) */
int binSearchPartitionToInsertInto(uint64_t candidateKey, Partition *sortedPartitions) {

    int low = 0;
    int high = numPartitions - 1;
    int mid = (high + low) / 2;

    int idxToReturn = 0; 

    while (low <= high) {

        if (candidateKey >= (sortedPartitions + mid)->minKey) { // go right
            idxToReturn = mid;
            low = mid + 1;
            mid = (high + low) / 2;
        }
        else { // go left
            high = mid - 1;
            mid = (high + low) / 2;
        }
    }
    return idxToReturn;
}

void insertAllRecordsIntoPartitions(Record* recordsBaseAddr, Partition *partitions) {

    cout << "Working... Inserting all Records (their key-ptr pairs) into respective Partitions\n";

    #pragma omp parallel for num_threads(numThreads)
    for (int i = 0; i < numKeysToSort; i++) {
        uint64_t keyToInsert = (recordsBaseAddr + i)->key;
        int targetIdx = binSearchPartitionToInsertInto(keyToInsert, partitions);
        insertBSTNode(keyToInsert, (recordsBaseAddr + i), partitions + targetIdx, targetIdx);    
    }

}

/* Helper for insertBSTNode method */
BSTKeyPtrPair* insertAtPosition(size_t position, BSTKeyPtrPair* toInsert, BSTKeyPtrPair* startOfRegion) {

    pmem_memcpy_nodrain((void* ) (startOfRegion + position), toInsert, sizeof(BSTKeyPtrPair));

    return (startOfRegion + position);

}

void insertBSTNode(uint64_t keyToInsert, Record* recordPtr, Partition *targetPartition, int targetPartitionIdx) {

    BSTKeyPtrPair nodeToInsert;
    nodeToInsert.key = keyToInsert;
    nodeToInsert.recordPtr = recordPtr;
    BSTKeyPtrPair* root = targetPartition->rootOfBST;

    targetPartition->mutex.lock();

    // If we run out of space, allocate new region!

    if (keyToInsert == root->key) {
        // No Duplicate Insertions allowed
        targetPartition->mutex.unlock();
        return;
    }


    if (targetPartition->currPoolNodes > 0 && targetPartition->currPoolNodes % nodesPerAllocation == 0) {
        //cout << "Reallocating ... currPoolNodes = " << targetPartition->currPoolNodes << endl;

        // Reallocate
        string partitionNameString(PARTITION_FILE_PATH_PREFIX);
        partitionNameString.append(to_string(targetPartitionIdx) + "_" + to_string(targetPartition->poolPtrs.size()));
        BSTKeyPtrPair* newRegionBaseAddr = allocateNVMRegion<BSTKeyPtrPair>(nodesPerAllocation * sizeof(BSTKeyPtrPair), partitionNameString.c_str());

        targetPartition->poolPtrs.push((char* ) newRegionBaseAddr);
        targetPartition->currPoolBaseAddr = (char* ) newRegionBaseAddr;
        //targetPartition->currPoolNodes = 0;
        
    }

    BSTKeyPtrPair* curr = root;


    int insertionIndex = targetPartition->currPoolNodes % nodesPerAllocation;

    while (true) {
        if (keyToInsert > curr->key) { // go right
            if (curr->right == nullptr) { // insert
                BSTKeyPtrPair* newNode = insertAtPosition(insertionIndex, &nodeToInsert, (BSTKeyPtrPair* ) targetPartition->currPoolBaseAddr); // nodeToInsert is on the stack memory. Careful!
                curr->right = newNode;
                break;
            }
            curr = curr->right;

        }
        else { // go left
            if (curr->left == nullptr) { // insert
                BSTKeyPtrPair* newNode = insertAtPosition(insertionIndex, &nodeToInsert, (BSTKeyPtrPair* ) targetPartition->currPoolBaseAddr); // nodeToInsert is on the stack memory. Careful!
                curr->left = newNode;
                break;
            }
            curr = curr->left;
        }
    }

    targetPartition->currPoolNodes++;
    targetPartition->mutex.unlock();

}

int inOrderTraversal(BSTKeyPtrPair* root, int startDisplacement) {

    if (root == nullptr) return startDisplacement;

    int currDisplacement = startDisplacement;

    if (root->left != nullptr) {
        currDisplacement = inOrderTraversal(root->left, startDisplacement);
    }

#if PRINT_DURING_INORDER_TRAVERSAL
    cout << "Key = " << root->key << endl;
#endif

    finalSortedPairs[currDisplacement].key = root->key;
    finalSortedPairs[currDisplacement].recordPtr = root->recordPtr;
    currDisplacement++;

    if (root->right != nullptr) {
        currDisplacement = inOrderTraversal(root->right, currDisplacement);
    }

    return currDisplacement;

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
