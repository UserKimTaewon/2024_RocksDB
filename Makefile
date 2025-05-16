CXX = g++
CXXFLAGS = -std=c++17 -Wall -I. -Inlohmann -Irocksdb-main/include

LDFLAGS = rocksdb-main/librocksdb.a -lmsgpackc -lpthread -lz -lbz2 -lsnappy -llz4 -lzstd

SRC = main.cpp LogDB.cpp ChronobreakFilter.cpp
TARGET = log_example

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(TARGET)
