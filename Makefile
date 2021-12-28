build_all:
	g++ -std=c++17 -o GenerateData GenerateData.cpp -fopenmp -lpthread -lpmem