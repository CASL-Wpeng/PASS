include ../common/Makefile_common
CFLAGS += -DINPLACE -I./_deps/epoch_reclaimer-src -Wno-long-long -march=native -Ofast -O3 -DNDEBUG -std=gnu++17
all:Spiral.so

Spiral.so:spiral_storage.cpp spiral_storage.h allocator.h Hash.h utils.h
	$(CXX) $(CFLAGS) -fPIC -shared $< -o $@ 
clean:
	rm -f *.o *.so lsb msb