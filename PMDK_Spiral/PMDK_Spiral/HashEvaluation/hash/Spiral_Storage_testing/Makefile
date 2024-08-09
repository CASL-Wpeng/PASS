CFLAGS = $(shell pkg-config --cflags libpmemobj) -O2
LDFLAGS = $(shell pkg-config --libs libpmemobj) -O2
CC = g++
RM = rm

all:Spiral_Storage_Testing.so

Spiral_Storage_Testing.so:spiral_storage.cpp spiral_storage.h 
	$(CXX) $(CFLAGS) -fPIC -shared $< -o $@  $(LDFLAGS)

clean: 
	rm -f *.o