CFLAGS = $(shell pkg-config --cflags libpmemobj) -O2
LDFLAGS = $(shell pkg-config --libs libpmemobj) -O2
CC = g++
RM = rm

all: Linear.so

Linear.so:linear.cpp linear.h 
	$(CXX) $(CFLAGS) -fPIC -shared $< -o $@  $(LDFLAGS)

clean: 
	rm -f *.o