# SplitSort - Sorting large files in the Intel Optane DCPMM

## Overview
This is a parallel sorting algorithm (tentatively called "SplitSort") that is intended for the systems that are NVM aware. Specifically, this code is designed to work with the Intel Optane DCPMM hardware, and
requires the **libpmem** library provided by the **PMDK** project. This project also requires **C++17** and utilizes the **OpenMP** library for parallelization. The code is intended
to run on a DRAM-NVM hybrid architecture, with the Intel Optane DCPMM configured to App-Direct mode to bypass the kernel space.\
\
This algorithm was created to minimize writes to the NVM hardware for wear-levelling, while at the same time being fast and efficient. This algorithm promises only ```O(n)``` writes to the NVM hardware, and in fact can be configured to utilize exactly ```n``` writes in the NVM hardware, given enough DRAM capacity.\
\
Detailed information about the setup, experiments and algorithm can be found in the reports located in the **Reports** folder of this repository.

## Requirements
libpmem (from PMDK: https://manpages.debian.org/testing/libpmem-dev/libpmem.7.en.html) \
C++17\
OpenMP\
pthreads

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

## Credits
Prof. Tan Kian Lee and Huang Wen Tao (of National University of Singapore) \
Koh Yi Da
