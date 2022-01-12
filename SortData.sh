# Usage: <num_keys_to_sort> <num_threads> <num_samples> <num_partitions>

# echo Sorting Generated Data.
# ./SplitSort.o 33554432 32 50000 2048
# echo Sorting Complete.

echo Sorting Generated Data.
./SplitSort.o 33554432 32 4096 512
echo Sorting Complete.
