
#include "spiral_storage.h"
#include "../../common/hash.h"
#include <vector>
#include <fstream>
// pool path and name
static const char *pool_name = "pmem_hash.data";
//static const char *pool_name = "../../../../../mnt/pmem0/wpeng/pmem_spiral.data";
// pool size
static const size_t pool_size = 1024ul * 1024 * 10ul;

#define READ_WRITE_NUM 500

const Value_t DEFAULT = reinterpret_cast<Value_t>(1);

struct my_root {
	size_t len;
	char buf[30];
};

static bool FileExists(const char *pool_path) {
  struct stat buffer;
  if (stat(pool_path, &buffer) == 0){
	remove(pool_name);
  }
  return 0;
}

const uint32_t kNumBucket = 32;
const uint32_t kFingerBits = 8;

constexpr uint32_t shiftBits = (31 - __builtin_clz(kNumBucket)) + kFingerBits;
constexpr size_t bucketMask = ((1 << (31 - __builtin_clz(kNumBucket))) - 1);

#define BUCKET_INDEX(hash) (((hash) >> (64 - shiftBits)) & bucketMask)
#define META_HASH(hash) ((uint8_t)((hash) >> (64 - kFingerBits)))



  void bin(uint64_t n)
  {
    uint64_t i;
    cout << "0";
    for (i = 1 << 30; i > 0; i = i / 2)
    {
      if((n & i) != 0)
      {
        cout << "1";
      }
      else
      {
        cout << "0";
      }
    }
	cout << endl;
  }

extern "C" hash_api *create_tree(const tree_options_t &opt, unsigned sz, unsigned tnum)
{
  if (sz)
    sz = sz;
  else
    sz = 65536;
  // Step 1: create (if not exist) and open the pool
  bool file_exist = false;
  if (FileExists(pool_name)) file_exist = true;
  
  if (!file_exist){
		PMEMobjpool *pop;
    	pop = pmemobj_create(pool_name, "Spiral Storage", 1024 * 1024 * 32UL, 0666);
    	if (pop == NULL) {
			perror(pool_name);
			exit(1);
		}
		    //size_t segment_number = (size_t)argv[1];
    size_t init_size = sz >= 2 ? sz : 2;
    PMEMoid root = pmemobj_root(pop, sizeof (SpiralStorage));
		struct SpiralStorage *rootp = (SpiralStorage*)pmemobj_direct(root);
    new (rootp) SpiralStorage();
		rootp->init_Spiral(pop,rootp,init_size);
		if (rootp == NULL)
		{
			perror("Pointer error");
			exit(1);
		}
  cout << "Return the pointer to Spiral Storage" << std::endl;
  return rootp;
  }
  cout << "error" << std::endl;
  return NULL;
}


inline int fastrand(unsigned int g_seed) { 
  g_seed = (214013*g_seed+2531011); 
  return (g_seed>>16)&0x7FFF; 
} 


uint64_t logical_to_physical(uint64_t logical){
    uint64_t high = (1 + logical)/2;
    uint64_t low = logical/2;
    uint64_t physical;
    if (low < high){
        physical = logical_to_physical(low);
    }
    else {
        physical = logical - low; 
    }
    return physical;
}


int main(int argc, char *argv[]){

	double single_time;
	struct timespec start, finish;
	int table[1000000];

	for(uint64_t i = 0; i < 1000000;i++){
	 	table[i] = logical_to_physical(i);
	}


// hash key

 	clock_gettime(CLOCK_MONOTONIC, &start);	
	for(int i = 0; i < 10000;i++){
		for(int j = 0; j < 10000; j++){
			int key = 1024*i +j;
			auto key_hash = h(&key, sizeof(key));
		}
	}
	clock_gettime(CLOCK_MONOTONIC, &finish);
	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
	std::cout << "MurMur Hash speed "  <<single_time << endl;
 

	uint64_t *hash_key_table;
	hash_key_table = (uint64_t *)malloc(sizeof(uint64_t) * 100000000);

	for(int i = 0; i < 10000;i++){
		for(int j = 0; j < 10000; j++){
			hash_key_table[i*10000 +j] = rand();
		}
	}

//spiral storage

	double c = log2(100000);

	// clock_gettime(CLOCK_MONOTONIC, &start);	

	// for(int i = 0; i < 10000;i++){
	// 	for(int j = 0; j < 10000; j++){
	// 		int key = 1024*i +j;
	// 		auto key_hash = h(&key, sizeof(key));
	// 		auto a = (double)key_hash/INT64_MAX/2;
	// 		double G = (int)(c - a) + 1 + a;
	// 		int logical = pow(2,G);
	// 		int physical = logical_to_physical(logical);
	// 	}
	// }

	// clock_gettime(CLOCK_MONOTONIC, &finish);
	// single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
	// std::cout << "original spiral storage with log2(100000) " <<single_time << endl;


//spiral storage with power table

   c = log2(100);

   int init = 1;
   uint32_t power_table[65536*16];
    for(int i = 0;i < 65536*16;i++){
        double temp = log2(init) * 65536;    
        if(i >= int(temp)){
            init++;
        }
        power_table[i] = init - 1;
//		std::cout << power_table[i] << endl;
    }

	clock_gettime(CLOCK_MONOTONIC, &start);	

	for(int i = 0; i < 10000;i++){
		for(int j = 0; j < 10000; j++){
			auto a = (double)hash_key_table[i]/RAND_MAX;
			double G = (int)(c - a) + 1 + a;
			int logical = power_table[int(G *65536)];
			int physical = logical_to_physical(logical);
			//std::cout << physical << std::endl;
		}
	}

	clock_gettime(CLOCK_MONOTONIC, &finish);

	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
	std::cout << "spiral storage with power table " <<single_time << endl;


// //spiral storage with look-up-table

	clock_gettime(CLOCK_MONOTONIC, &start);	

	for(int i = 0; i < 10000;i++){
		for(int j = 0; j < 10000; j++){
			auto a = (double)hash_key_table[i]/RAND_MAX;
			double G = (int)(c - a) + 1 + a;
			int logical = pow(2,G);
			int physical = table[logical];
		}
	}

	clock_gettime(CLOCK_MONOTONIC, &finish);

	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
	std::cout << "spiral storage with look up table " <<single_time << endl;

//spiral storage with all


	clock_gettime(CLOCK_MONOTONIC, &start);	

	for(int i = 0; i < 10000;i++){
		for(int j = 0; j < 10000; j++){
			auto a = (double)hash_key_table[i]/RAND_MAX;
			double G = (int)(c - a) + 1 + a;
			int logical = power_table[int(G *65536)];
			int physical = table[logical];
		}
	}

	clock_gettime(CLOCK_MONOTONIC, &finish);

	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
	std::cout << "spiral storage with all tables " <<single_time << endl;


	uint32_t *power_table2;
	power_table2 = (uint32_t *)malloc(sizeof(uint32_t) * 65536*16);

	init = 1;

    for(int i = 0;i < 65536*16;i++){
        double temp = log2(init) * 65536;    
        if(i >= int(temp)){
            init++;
        }
		int logical = init -1;
		int physical = table[logical];
        power_table2[i] = physical;
    }

	clock_gettime(CLOCK_MONOTONIC, &start);	

	for(int i = 0; i < 10000;i++){
		for(int j = 0; j < 10000; j++){
			auto a = (double)hash_key_table[i]/RAND_MAX;
			double G = (int)(c - a) + 1 + a;
			int physical = power_table2[int(G *65536)];
		}
	}

	clock_gettime(CLOCK_MONOTONIC, &finish);

	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
	std::cout << "spiral storage with new table " << single_time << endl;

	// std::cout <<std::numeric_limits<double>::is_iec559 << endl;

    // init = 1;
    // uint64_t a[256*16] = {0};
    // for(int i = 0;i < 256*16;i++){
    //     double result = i;
    //     double mark = 256 * log2(init);    
    //     if(result >= (int)mark){
    //         init++;
    //     }
    //     a[i] = init - 1 ;
    // }
    // double test = 6.23;
    // int idx = test*256;
	// cout << idx << endl;
    // cout << a[idx] << endl;
   
    // for(int i = 1583; i < 1595;i++){
	// 	cout << i << " is " << a[i] << endl;
	// }


	// std::ofstream myfile;
	// myfile.open ("Power_table.txt");
    // for(int i = 0; i < 256*16;i++){
	// 	myfile << a[i] << std::endl;
	// }
    // myfile.close();


	return 0;

}