
#echo Generating 16GB of Records, 32B per Record, SEED = 1234
#./GenerateData 536870912 1234 # 32 bytes Record, 16GB total size. 2^29 Records

echo Generating 1GB of Records, 32B per Record, SEED = 1234
echo
./GenerateData 33554432 1234 # 32 bytes Record, 1GB total size. 2^25 Records, SEED = 1234