# Usage: <num_keys_to_sort> <num_threads> <num_samples> <num_partitions>

# echo Sorting Generated Data.
# ./SplitSort.o 33554432 32 50000 2048
# echo Sorting Complete.

echo Sorting Generated Data.
./SplitSort.o 64 4 16 4
echo Sorting Complete.
