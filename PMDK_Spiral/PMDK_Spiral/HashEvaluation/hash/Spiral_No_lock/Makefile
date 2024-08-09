CFLAGS = $(shell pkg-config --cflags libpmemobj) -O2
LDFLAGS = $(shell pkg-config --libs libpmemobj) -O2
CC = g++
RM = rm

%.o: %.cpp
	$(CC) -c $< -o $@ $(CFLAGS)

spiral_no_lock.so: spiral_storage.o
	 $(CC) -o $@ $< $(LDFLAGS)

clean: 
	rm -f *.o