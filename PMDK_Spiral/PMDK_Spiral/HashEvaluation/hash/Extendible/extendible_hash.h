#ifndef EXTENDIBLE_PTR_H_
#define EXTENDIBLE_PTR_H_

#include <cstring>
#include <vector>
#include "../common/hash.h"
#include "../common/hash_api.h"
#include "../common/pair.h"
#include "../common/timer.h"
#include <libpmemobj.h>


#define LSB
const size_t kMask = 256-1;
const size_t kShift = 8;


TOID_DECLARE(char,16);

using namespace std;



void *cache_align(void *ptr) {
  uint64_t pp = (uint64_t)ptr;
  pp += 48;
  return (void *)pp;
}

#define CAS(_p, _u, _v)                                             \
  (__atomic_compare_exchange_n(_p, _u, _v, false, __ATOMIC_ACQUIRE, \
                               __ATOMIC_ACQUIRE))


struct Block {
   static const size_t kBlockSize = 256; // 4 - 1
  // static const size_t kBlockSize = 1024; // 16 - 1
  // static const size_t kBlockSize = 4*1024; // 64 - 1
  // static const size_t kBlockSize = 16*1024; // 256 - 1
  //static const size_t kBlockSize = 64*1024; // 1024 - 1
  //static const size_t kBlockSize = 256*1024; // 4096 - 1
  static const size_t kNumSlot = kBlockSize/sizeof(Pair);

  Block(void)
  : local_depth{0}
  { 
  }

  Block(size_t depth)
  :local_depth{depth}
  { 
     //cout << "new Block with depth" <<endl;
    PMEMoid __ = pmemobj_tx_zalloc(sizeof(Pair) * kNumSlot, TOID_TYPE_NUM(char));
    _ = (Pair *)cache_align(pmemobj_direct(__));
    for (int j = 0; j <  kNumSlot ; j++){
          _[j].key = INVALID;
       }
  }

  ~Block(void) {
  }


  int Insert(PMEMobjpool *pop,Key_t&, Value_t, size_t);
  void Insert4split(PMEMobjpool *pop,Key_t&, Value_t);
  bool Put(PMEMobjpool *pop,Key_t&, Value_t, size_t);
  Block** Split(PMEMobjpool *pop);

  Pair *_;
  size_t local_depth;
  int64_t sema = 0;
  size_t pattern = 0;
  size_t numElem(void); 
};

struct Directory {
  static const size_t kDefaultDirectorySize = 256;
  Block** _;
  PMEMoid __;
  size_t capacity;
  bool lock;
  int sema = 0 ;

  Directory(void) {
    capacity = kDefaultDirectorySize;
     //cout << "testing Direcotry 1" <<endl;
    __ = pmemobj_tx_zalloc(sizeof(Block) * capacity, TOID_TYPE_NUM(char));
    _ = (Block **)cache_align(pmemobj_direct(__));
     //cout << "testing Direcotry 2" <<endl;
    lock = false;
    sema = 0;
  }

  Directory(size_t size) {
    capacity = size;
    __ = pmemobj_tx_zalloc(sizeof(Block) * capacity, TOID_TYPE_NUM(char));
    _ = (Block **)cache_align(pmemobj_direct(__));
    lock = false;
    sema = 0;
  }

  ~Directory(void) {
    delete [] _;
  }

  bool Acquire(void) {
    bool unlocked = false;
    return CAS(&lock, &unlocked, true);
  }

  bool Release(void) {
    bool locked = true;
    return CAS(&lock, &locked, false);
  }
  
  void SanityCheck(void*);
  void LSBUpdate(PMEMobjpool *pop,int, int, int, int, Block**);
};

class ExtendibleHash : public hash_api {
  public:
    ExtendibleHash(void);
    ExtendibleHash(size_t);
    ~ExtendibleHash(void);
    void init_Extend(PMEMobjpool *_pop, ExtendibleHash *extend,size_t init_size);
    bool Insert(Key_t&, Value_t);
    bool InsertOnly(Key_t&, Value_t);
    bool Delete(Key_t&);
    Value_t Get(Key_t&);
    Value_t FindAnyway(Key_t&);
    double Utilization(void);
    size_t Capacity(void);
    bool Recovery();
  std::string hash_name() { return "Extendible"; };

  bool find(const char *key, size_t key_sz, char *value_out, unsigned tid)
  {
    Key_t k = *reinterpret_cast<const Key_t *>(key);
    auto r = Get(k);

    return r;
  }

  bool insert(const char *key, size_t key_sz, const char *value,
              size_t value_sz, unsigned tid, unsigned t)
  {
    Key_t k = *reinterpret_cast<const Key_t *>(key);
    Insert(k, value);
    return true;
  }
  bool insertResize(const char *key, size_t key_sz, const char *value,
                    size_t value_sz)
  {
    Key_t k = *reinterpret_cast<const Key_t *>(key);
    return Insert(k, value);
  }
  bool update(const char *key, size_t key_sz, const char *value,
              size_t value_sz)
  {
    return true;
  }

  bool remove(const char *key, size_t key_sz, unsigned tid)
  {
    Key_t k = *reinterpret_cast<const Key_t *>(key);
    return Delete(k);
  }

  int scan(const char *key, size_t key_sz, int scan_sz, char *&values_out)
  {
    return scan_sz;
  }
  bool recovery()
  {
    Recovery();
    return true;
  }
  PMEMobjpool *pop;
  size_t global_depth;
  Directory *dir;
  Timer timer;
  double breakdown = 0;
};

#endif  // EXTENDIBLE_PTR_H_
