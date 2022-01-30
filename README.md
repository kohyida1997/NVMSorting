# NVMSorting

## Overview
This is a parallel sorting algorithm (tentatively called "SplitSort") that is intended for the systems that are NVM aware. Specifically, this code is designed to work with the Intel Optane DCPMM hardware, and
requires the **libpmem** library provided by the **PMDK** project. This project also requires **C++17** and utilizes the **OpenMP** library for parallelization. The code is intended
to run on a DRAM-NVM hybrid architecture, with the Intel Optane DCPMM configured to App-Direct mode to bypass the kernel space.\
\
Detailed information about the setup, experiments and algorithm can be found in the reports located in the **Reports** folder of this repository.

## Running the code

**Note: Please change the target file paths in ```GenerateData.cpp``` and ```SplitSort.cpp``` before attempting to run the bash scripts**

### 1. Generating data to be sorted in NVM
**Generated data should be on NVM, and is currently set to "/dcpmm/yida/UNSORTED_KEYS" by default**. By default, the script creates a 16GB (2^29 items) sized file of 32-byte sized Records (key, val) in NVM.\
Usage:\
```bash GenerateData.sh```

### 2. Running the algorithm on the generated data
**Target file to sort should be on NVM, and needs to match the generated data's file path.** The number of items to be sorted needs to be specified as command line arguments to the program. Please edit the ```SortData.sh``` bash file directly if you wish to run different experimental setups.\
Usage:\
```bash SortData.sh``` 
