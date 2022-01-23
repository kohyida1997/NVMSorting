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

/* This is the path to the Unsorted file, that SHOULD be in NVM (dcpmm directory is NVM storage media) */
static const char* UNSORTED_FILE_PATH = "/dcpmm/yida/UNSORTED_KEYS";

/* This is the prefix for the temporary partitions we will create in this algorithm, that should ALSO be in NVM storage media*/
static const char* PARTITION_FILE_PATH_PREFIX = "/dcpmm/yida/PARTITION";

/* Number of threads to use when running SplitSort. */
static unsigned int numThreads;

/* Number of Records to sample out of ALL unsorted Records. Keep in mind that these samples will be stored in DRAM, NOT NVM. */
static unsigned int numSamples;

/* Number of partitions we will create in this run of the SplitSort algorithm. */
static unsigned int numPartitions;

/* A temporary array to store the final sorted pairs after the algorithm is done. 

- What are KeyPtrPairs?
> Instead of sorting the Record data structures, we sort a pair (Key, Ptr) where
  key is the for the Record, and ptr is a POINTER to the Record object. Ie. (Record *)

*/
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

/* Number of Records to sort needs to be provided. */
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

    /* Setup the important metadata using command line args. */

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

    /* Map the unsorted Records into memory so that it is easier to operate on them. */

    Record* recordBaseAddr = mmapUnsortedFile();

    /* Set up the final array to store the sorted (Key, Record *) pairs*/

    finalSortedPairs = new KeyPtrPair[numKeysToSort];

#if PRINT_UNSORTED_KEYS
    /* To be used for sanity checks only */
    for (int i = 0; i < numKeysToSort; i++) {
        cout << (recordBaseAddr + i)->key << endl;
    }    
#endif

#if PRESORT_DATA_FOR_TESTING
    /* To be used for sanity checks only */
    sort(recordBaseAddr, recordBaseAddr + numKeysToSort, [](Record x, Record y) {return x.key < y.key;});
#endif
    splitSort(recordBaseAddr); 

#if CHECK_KEYS_ARE_SORTED
    /* Verify that the sorting algorithm is CORRECT.  */
    cout << "Working... Verifying keys are correctly sorted" << endl;

    int errorRegister = 0;

    #pragma omp parallel for num_threads(64) 
    for (int i = 1; i < numKeysToSort; i++) {
        if ((finalSortedPairs + i)->key < (finalSortedPairs + i - 1)->key || errorRegister != 0) {
            cout << "!!! Critical Failure. Sorting is incorrect !!!\n";
            errorRegister++;
            exit(1);
        }
    }

    if (errorRegister > 0) {
        cout << "!!! Critical Failure. Sorting is WRONG !!!\n";
        exit(1);
    }

    cout << "Working... Success, Keys are in sorted ascending order! ✓ \n";
#endif

    /* Cleanup */
    delete[] finalSortedPairs;

    return 0;

}

void splitSort(Record* recordsBaseAddr) {

    // Sample records (samples are stored in DRAM)
    vector<KeyPtrPair>* sampledKeys = new vector<KeyPtrPair>();
    systematicParSample(recordsBaseAddr, sampledKeys);

    // Sort samples (this is all done in DRAM)
    stdSortSamples(sampledKeys);

    // Create and initialize partitions
    /* Note: Partition metadata is stored in DRAM. But the actual KeyPtr data is stored in NVM */
    Partition *partitions = new Partition[numPartitions];
    parPartitionSamples(sampledKeys, partitions);

    // Insert into partitions (partitions data is in NVM, so we are inserting into NVM)
    insertAllRecordsIntoPartitions(recordsBaseAddr, partitions);

    // Read out the partitions (note that all partitions are sorted relative to each other. ie. All keys in Partition0 are smaller than all keys in Partition1 and so on.)

    // Sub-task: Compute prefix sums sequentially.
    long rollingSum = 0;
    vector<long> startDisplacement(numPartitions);
    startDisplacement[0] = 0;
    rollingSum += partitions[0].currPoolNodes;
    for (int i = 1; i < numPartitions; i++) {
        startDisplacement[i] = rollingSum;
        rollingSum += partitions[i].currPoolNodes;
    }

    // Do in-order traversal of each partition in parallel after we have the prefix sums.
    #pragma omp parallel for num_threads(numThreads)
    for (int i = 0; i < numPartitions; i++)
        inOrderTraversal(partitions[i].rootOfBST, startDisplacement[i]);

    // Cleanup (NOTE: need to unmap all the mapped files too)
    delete sampledKeys;
    delete[] partitions;

}

/* Perform systematic sampling of the unsorted Records, put them into sampledKeys vector. (Parallel) */
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
    /* To be used for sanity checks only */
    cout << "Printing... Sampled Keys\n";
    auto temp = *sampledKeys;
    for (int i = 0; i < numSamples; i++) {
        cout << "Sample " << i << ": " << temp[i].key << endl;
    }
#endif

}

/* Sort all the sampled keys using STL sort. (Sequential) */
void stdSortSamples(vector<KeyPtrPair>* sampledKeys) {
    KeyPtrPair* start = &(*sampledKeys)[0];
    sort(start, start + numSamples, [](KeyPtrPair x, KeyPtrPair y) {return x.key < y.key;});

#if PRINT_SORTED_SAMPLED_KEYS 
    /* To be used for sanity checks only */
    cout << "Printing... Sorted Sampled Keys\n";
    auto temp = *sampledKeys;
    for (int i = 0; i < numSamples; i++) {
        cout << "Sample " << i << ": " << (*sampledKeys)[i].key << endl;
    }
#endif

}

/* Create partitions out of the already sorted sampledKeys. Partitions are roughly the same size. (Parallel)*/
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

/* Each partition is essentially a contiguous range of items in sampledKeys, specified by BEGIN and END (exclusive). Here we initialize the metadata for one partition. (Sequential) */
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

    // Naming convention for the NVM files opened for each partition is eg. "PARTITION5_1" and "PARTITION5_2" and so on. 
    partitionNameString.append(to_string(index) + "_" + to_string(0));
    BSTKeyPtrPair* partitionBaseAddr = allocateNVMRegion<BSTKeyPtrPair>(nodesPerAllocation * sizeof(BSTKeyPtrPair), partitionNameString.c_str());

    // Insert the middle element as ROOT
    pmem_memcpy_nodrain((void*) partitionBaseAddr, (void*) &root, sizeof(BSTKeyPtrPair));
    targetPartition->rootOfBST = partitionBaseAddr;

    targetPartition->currPoolNodes = 1;
    targetPartition->currPoolBaseAddr = (char*) partitionBaseAddr;
    targetPartition->poolPtrs.push((char*) partitionBaseAddr);

#if PRINT_PARTITION_INFO
    /* To be used for sanity checks only */
    cout << "Partition " << index << ": " << (end - begin) << " elements. [" << begin << ", " << end << "] Root key = " << partitionBaseAddr->key << "\n";
#endif
}

/* Returns the index of the correct Partition to insert into using a Binary Search. (non-recursive) */
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

/* After creating and initializing all the partitions, we start inserting ALL the original unsorted records into their correct partitions. (Parallel)*/
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

/* Each Partition essentially holds a single Binary Search Tree (BST). This method helps us to insert a ney Key into the BST at this partition. (Sequential) */
void insertBSTNode(uint64_t keyToInsert, Record* recordPtr, Partition *targetPartition, int targetPartitionIdx) {

    BSTKeyPtrPair nodeToInsert;
    nodeToInsert.key = keyToInsert;
    nodeToInsert.recordPtr = recordPtr;
    BSTKeyPtrPair* root = targetPartition->rootOfBST;

    // Multiple threads can access the same BST concurrently, so we need locking.
    targetPartition->mutex.lock();

    // If we run out of space, allocate new region!

    if (keyToInsert == root->key) {
        // No Duplicate Insertions allowed
        targetPartition->mutex.unlock();
        return;
    }


    if (targetPartition->currPoolNodes > 0 && targetPartition->currPoolNodes % nodesPerAllocation == 0) {

        // Reallocate
        string partitionNameString(PARTITION_FILE_PATH_PREFIX);
        partitionNameString.append(to_string(targetPartitionIdx) + "_" + to_string(targetPartition->poolPtrs.size()));
        BSTKeyPtrPair* newRegionBaseAddr = allocateNVMRegion<BSTKeyPtrPair>(nodesPerAllocation * sizeof(BSTKeyPtrPair), partitionNameString.c_str());

        targetPartition->poolPtrs.push((char* ) newRegionBaseAddr);
        targetPartition->currPoolBaseAddr = (char* ) newRegionBaseAddr;
        
    }

    BSTKeyPtrPair* curr = root;

    // A little hack to get the insertion index (the BST nodes are actually stored as contiguous memory)
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

/* Perform an in-order traversal, recursively of a particular BST starting from the root, and inserting the accessed nodes into the final sorted array. */
int inOrderTraversal(BSTKeyPtrPair* root, int startDisplacement) {

    if (root == nullptr) return startDisplacement;

    int currDisplacement = startDisplacement;

    if (root->left != nullptr) {
        currDisplacement = inOrderTraversal(root->left, startDisplacement);
    }

#if PRINT_DURING_INORDER_TRAVERSAL
    /* To be used for sanity checks only */
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

/* Utility method to map the unsorted Records into memory. */
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
