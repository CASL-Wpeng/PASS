CFLAGS = $(shell pkg-config --cflags libpmemobj) -O2
LDFLAGS = $(shell pkg-config --libs libpmemobj) -O2
CC = g++
RM = rm

all: Extendible.so

Extendible.so:extendible_hash.cpp extendible_hash.h 
	$(CXX) $(CFLAGS) -fPIC -shared $< -o $@  $(LDFLAGS)

clean: 
	rm -f *.o