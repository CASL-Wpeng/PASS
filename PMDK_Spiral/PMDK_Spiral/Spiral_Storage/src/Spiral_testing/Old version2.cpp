
#include "spiral_storage.h"
#include "XoshiroCpp.hpp"
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
	int table[1000000] = {0};
	double power_table[300] = {0};
	
	double sum = 0;
	for(int i = 0;i < 256; i ++){
		power_table[i] = log2(i);
	}
	std::vector<double> power_table_vector;
	power_table_vector.insert(power_table_vector.begin(),std::begin(power_table),std::end(power_table));
	//memcpy(&power_table_vector[0],&power_table[0],sizeof(double)*300);


	double c = log2(100000);
	double c2 = c - int(c);
	int l1 = pow(2,int(c));

	clock_gettime(CLOCK_MONOTONIC, &start);	

	for(int i = 0; i < 10000;i++){
		for(int j = 0; j < 10000; j++){
			uint64_t key = 1024*i +j;
			double a;
			memcpy(&a,&key,sizeof(key));
			double G = (int)(c - a) + 1 + a;
			int logical = std::lower_bound(power_table_vector.begin(),power_table_vector.end(),G) - power_table_vector.begin();
			int physical = table[logical];
		}
	}


	clock_gettime(CLOCK_MONOTONIC, &finish);
	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
	std::cout << "new spiral storage with log2(100000) + new G function + vector + tables + memcpy() " <<  single_time << endl;	


	clock_gettime(CLOCK_MONOTONIC, &start);	

	for(int i = 0; i < 10000;i++){
		for(int j = 0; j < 10000; j++){
			uint64_t key = 1024*i +j;
			double a;
			memcpy(&a,&key,sizeof(key));
			double G = (int)(c2 - a) + 1 + a;
			int logical = l1 * exp2(G);
			int physical = table[logical];
		}
	}

	clock_gettime(CLOCK_MONOTONIC, &finish);
	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
	std::cout << "new spiral storage with log2(100000) + new G function + table + memcpy() + split " <<  single_time << endl;

   int init = 1;
   uint32_t power_table2[65536*16] = {0};
    for(int i = 0;i < 65536*16;i++){
        double result = (double)i/65536;
        double mark = log2(init);    
        if(result > mark){
            init++;
        }
        power_table2[i] = init;
    }

	clock_gettime(CLOCK_MONOTONIC, &start);	

	for(int i = 0; i < 10000;i++){
		for(int j = 0; j < 10000; j++){
			uint64_t key = 1024*i +j;
			double a;
			memcpy(&a,&key,sizeof(key));
			double G = (int)(c - a) +1 + a;
			int logical = power_table2[(int)(G*300)];
			int physical = table[logical];
		}
	}

	clock_gettime(CLOCK_MONOTONIC, &finish);
	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
	std::cout << "new spiral storage with log2(100000) + new G function + table + memcpy() + app table" <<  single_time << endl;


	clock_gettime(CLOCK_MONOTONIC, &start);	


	double v_1 = 27.7280233;
	double v_2 = 4.84252568;
	double v_3 = -0.49012907;
	double v_4 = -5.7259425;


	for(int i = 0; i < 10000;i++){
		for(int j = 0; j < 10000; j++){
			uint64_t key = 1024*i +j;
			double a;
			memcpy(&a,&key,sizeof(key));
			double G = (int)(c2 - a) + 1 + a;
			double G_app = 1 + v_1/(v_2 - G) + v_3 * G + v_4;
			int logical = l1 * G_app;
			int physical = table[logical];
		}
	}

	clock_gettime(CLOCK_MONOTONIC, &finish);
	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
	std::cout << "new spiral storage with log2(100000) + new G function + table + memcpy() + app " <<  single_time << endl;

	double G = 0.5;
	double G_app = 1 + v_1/(v_2 - G) + v_3 * G + v_4;
	std::cout << G_app << std::endl;

	v_1 = 3.4142136;
	v_2 = 8.2426407;
	v_3 = -1.4142136;

	for(int i = 0; i < 10000;i++){
		for(int j = 0; j < 10000; j++){
			uint64_t key = 1024*i +j;
			double a;
			memcpy(&a,&key,sizeof(key));
			double G = (int)(c2 - a) +1 + a;
			double G_app = v_1/(v_2-G) + v_3;
			int logical = l1 * G_app;
			int physical = table[logical];
		}
	}

	clock_gettime(CLOCK_MONOTONIC, &finish);
	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
	std::cout << "new spiral storage with log2(100000) + new G function + table + memcpy() + app in paper " <<  single_time << endl;



	std::ofstream myfile;
	myfile.open ("Look_up_table.txt");
    for(int i = 0; i < 100000;i++){
		uint64_t physical = logical_to_physical(i);
		myfile << "logical address " << i << " mapping to physical " << physical << std::endl;
	}
    myfile.close();


	uint64_t result = 0;
	for(int i = 0; i < 256;i++){
		for(int j = 0; j < 256; j++){
			uint64_t key = (double)result/65536;
			double a;
			memcpy(&a,&key,sizeof(key));
			double G = (int)(c - a) +1 + a;
			int logical = power_table2[(int)(G*300)];
			int physical = table[logical];
		}
	}

	
	

	return 0;

}