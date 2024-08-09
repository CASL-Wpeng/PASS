
#include "spiral_storage.h"
#include "../../common/hash.h"
#include "zipfian_int_distribution.hpp"
#include <vector>
#include <fstream>
#include <random>
#include <bitset>

// pool path and name
static const char *pool_name = "pmem_hash.data";
//static const char *pool_name = "../../../../../mnt/pmem0/wpeng/pmem_spiral.data";
// pool size
static const size_t pool_size = 1024ul * 1024 * 10ul;

#define READ_WRITE_NUM 500

const Value_t DEFAULT = reinterpret_cast<Value_t>(1);


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

double zipfianDistribution(double alpha, int n) {
    static std::random_device rd;
    static std::mt19937 gen(rd());
   
    std::uniform_real_distribution<double> dis(0.0, 1.0);

    double sum = 0.0;
    for (int i = 1; i <= n; ++i) {
        sum += 1.0 / std::pow(i, alpha);
    }

    double zipfian_value = 0.0;
    double target = dis(gen) * sum;

    for (int i = 1; i <= n; ++i) {
        zipfian_value += 1.0 / std::pow(i, alpha);
        if (zipfian_value >= target) {
            return static_cast<double>(i) / n;
        }
    }

    // This should not be reached
    return 0.0;
}

void showBits(const long double& value) {
    // Create a pointer to the long double variable
    const unsigned char* bytes = reinterpret_cast<const unsigned char*>(&value);

    // Display each byte and its bits
    for (int i = sizeof(long double) - 1; i >= 0; --i) {
        for (int j = 7; j >= 0; --j) {
            std::cout << ((bytes[i] >> j) & 1);
        }
        std::cout << ' ';
    }
    std::cout << std::endl;
}

struct string_key{
    int length;
    char key[0];
};

struct converter{
    long double ld;
    char bits[0];
};


class zipfian_key_generator_t{
public:
    zipfian_key_generator_t(size_t start, size_t end, float skew) : dist_(start, end, skew), generator_(time(0)){

    }

    uint64_t next_uint64(){
        return dist_(generator_);
    }

private:
    std::default_random_engine generator_;
    zipfian_int_distribution<uint64_t> dist_;
};



int main(int argc, char *argv[]){
  // Example usage
	double single_time;
	struct timespec start, finish;


  char charArray[] = "3.1415926535891";

  std::cout << sizeof(charArray) << std::endl;
  std::cout << sizeof(long double) << std::endl;

  long double myLongDouble = 3.141592653589793238L;
  showBits(myLongDouble);

  const int bufferSize = 16;

  std::snprintf(charArray, bufferSize, "%Lf", myLongDouble);

  std::cout << "Char array value: " << charArray << std::endl;

  long double longDoubleValue = std::strtold(charArray, nullptr);

  showBits(longDoubleValue);

  converter * con1 = reinterpret_cast<converter*>(malloc(sizeof(converter) + bufferSize));

  con1->ld = myLongDouble;

  std::strcpy(charArray, con1->bits);
  
  longDoubleValue = std::strtold(charArray, nullptr);

  showBits(longDoubleValue);

  converter * con2 = reinterpret_cast<converter*>(malloc(sizeof(converter) + bufferSize));

  //std::strcpy(con2->bits, charArray);

  memcpy(con2->bits,charArray,sizeof(long double));

  
  showBits(con2->ld);

  std::cout << con1->ld << std::endl;

  std::cout << con2->ld << std::endl;

  union {
        long double ld;
        uint64_t bits[2];
  } converter2;



  memcpy(converter2.bits,con1->bits,sizeof(long double));

  showBits(converter2.ld);


	clock_gettime(CLOCK_MONOTONIC, &start);	

	for(int i = 0; i < 10000;i++){
		for(int j = 0; j < 10000; j++){
			int key = 1024*i +j;
			auto a = (long double)key/INT64_MAX/2;

		}
	}

	clock_gettime(CLOCK_MONOTONIC, &finish);


  return 0;

}