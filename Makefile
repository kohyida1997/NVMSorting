build_all:
	g++ -std=c++17 -o GenerateData.o GenerateData.cpp -fopenmp -lpthread -lpmem