
#ifndef SS_H_
#define SS_H_

#include <stdint.h>

#include <shared_mutex>
#include <inttypes.h>
#include <libpmem.h>
#include <libpmemobj.h>
#include <stdint.h>
#include <sys/stat.h>

#include <atomic>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include "../../common/hash.h"
#include "../../common/hash_api.h"


typedef size_t Key_t;
typedef const char* Value_t;

const Key_t SENTINEL = -2;  // 11111...110
const Key_t INVALID = -1;   // 11111...111

#define KEY_LEN 8
#define VALUE_LEN 8

const Value_t NONE = 0x0;

TOID_DECLARE(char,16);

using namespace std;

#define ASSOC_NUM 15

#define STEP 4

#define TABLE_SIZE 1000000

#define LOOK_UP_TABLE true

#define LOG_FATAL(msg)      \
  std::cout << msg << "\n"; \
  exit(-1)


inline bool var_compare(char *str1, char *str2, int len1, int len2) {
  if (len1 != len2) return false;
  return !memcmp(str1, str2, len1);
}

void *cache_align(void *ptr) {
  uint64_t pp = (uint64_t)ptr;
  pp += 48;
  return (void *)pp;
}

struct Entry
{
  Key_t key;
  Value_t value;
  Entry()
  {
    key = INVALID;
    value = NONE;
  }
};

struct Node
{
  uint8_t token[ASSOC_NUM];
  Entry slot[ASSOC_NUM];
};


class SpiralStorage : public hash_api{
public:
  PMEMobjpool *pop;
  Node **buckets;

  uint64_t Spiral_num;

  uint64_t capacity;
  //    uint32_t occupied;
  uint64_t seed;
  uint64_t size;
  uint8_t status;
  uint32_t item_num;
  uint64_t *look_up_table;
  PMEMoid _look_up_table; 
  PMEMoid _new_buckets; 
  Node * new_buckets;
  uint64_t size_of_table;
  uint64_t expand_location;
  uint64_t thread_num = 1; 
  double c;
  double b;
  double threshold = 0.8;

  PMEMoid _mutex;
  PMEMoid _old_mutex; /*old mutex lock*/
  PMEMrwlock *mutex;

  int nlocks;
  uint64_t locksize;
  bool resizing;
  atomic<int64_t> resizing_lock;

  void resize(PMEMobjpool *pop);
  void generate_seeds()
  {
    srand(time(NULL));
    seed = rand();
  }
  double Transfer_HASH(Key_t);
  uint64_t Transfer_IDX(double hashkey);
  uint8_t logical_to_physical(uint8_t logical);
  SpiralStorage(void) {}
  SpiralStorage(size_t init_size,PMEMobjpool *_pool);
  ~SpiralStorage(void) {}
  bool InsertOnly(Key_t &, Value_t);
  bool Insert(Key_t &, Value_t);
  bool Update(Key_t &, Value_t);
  bool Delete(Key_t &);
  bool Recovery();
  void init_Spiral(PMEMobjpool *_pool, SpiralStorage *spiral,size_t init_size);
  uint8_t table_construction();
  uint8_t table_reconstruction();
  uint8_t table_construction_in_PM();
  Value_t Get(Key_t &);
  bool Positive_Get(Key_t &);
  bool Negative_Get(Key_t &);
  hash_Utilization Utilization(void);
  // hash_api
  void vmem_print_api() { cout<< "empty API" << endl; };
  string hash_name() { string name = "Spiral Storage"; return name; };
  void Show_Look_Up_Table(){
	int i;
	for (i = 0;i < size_of_table;i++){
		cout << i << "-th logical address maps to " << look_up_table[i] <<endl;
	  }
  }
  void print_all(){
    int i,j;
    int count =0;
    for(i = 0; i < size ; i++){
      for(j = 0; j < ASSOC_NUM;j++){
        if(buckets[i]->token[j]!= 0){
          cout << "key of buckets " << i << " slot " << j << " key is " << buckets[i]->slot[j].key << endl;
          count++;
        }
      }
    }
    cout << "print all size is " << size << endl; 
    cout << "the item number of spiral storage is " << item_num << endl;
    cout << "the true item number of spiral storage is " << count << endl;
  }

  bool recovery()
  {
    Recovery();
    return true;
  }
  hash_Utilization utilization() { return Utilization(); }
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
    Key_t k = *reinterpret_cast<const Key_t *>(key);
    return Update(k, value);
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
  string my_name(){return "SS";}
};

typedef struct thread_queue{
    Key_t key;
    uint8_t operation;                    // 0: read, 1: insert;
} thread_queue;

typedef struct sub_thread{
    pthread_t thread;
    uint32_t id;
    uint64_t inserted;
    uint64_t searched;
    SpiralStorage *spiral;
    thread_queue* run_queue;
} sub_thread;


uint32_t murmur3_32(const char *key, uint32_t len, uint32_t seed) {
    static const uint32_t c1 = 0xcc9e2d51;
    static const uint32_t c2 = 0x1b873593;
    static const uint32_t r1 = 15;
    static const uint32_t r2 = 13;
    static const uint32_t m = 5;
    static const uint32_t n = 0xe6546b64;

    uint32_t hash = seed;

    const int nblocks = len / 4;
    const uint32_t *blocks = (const uint32_t *) key;
    int i;
    for (i = 0; i < nblocks; i++) {
        uint32_t k = blocks[i];
        k *= c1;
        k = (k << r1) | (k >> (32 - r1));
        k *= c2;

        hash ^= k;
        hash = ((hash << r2) | (hash >> (32 - r2))) * m + n;
    }

    const uint8_t *tail = (const uint8_t *) (key + nblocks * 4);
    uint32_t k1 = 0;

    switch (len & 3) {
    case 3:
        k1 ^= tail[2] << 16;
    case 2:
        k1 ^= tail[1] << 8;
    case 1:
        k1 ^= tail[0];

        k1 *= c1;
        k1 = (k1 << r1) | (k1 >> (32 - r1));
        k1 *= c2;
        hash ^= k1;
    }

    hash ^= len;
    hash ^= (hash >> 16);
    hash *= 0x85ebca6b;
    hash ^= (hash >> 13);
    hash *= 0xc2b2ae35;
    hash ^= (hash >> 16);

    return hash;
}


double SpiralStorage::Transfer_HASH(Key_t key) {   
    //cout << "Transfer Hash start" << endl;
    //double hash_value =(murmur3_32((char*)&key, sizeof(Key_t),seed)*1.0/INT32_MAX);
    //double hash_value = key/INT32_MAX;
    //cout << "key is " << key << endl;
    //srand(key);
    //double hash_value = rand();
    //return hash_value/RAND_MAX;
    auto hash_value = h(&key, sizeof(key));
    return (double)hash_value/INT64_MAX/2;
}

uint64_t SpiralStorage::Transfer_IDX(double hashKey) {
    //printf("hash key is %f \n",hashKey);
    double G = (int)(c - hashKey) + 1 + hashKey;
    //printf("G is %f \n",G);
    return (uint64_t)pow(b,G);
}

uint8_t SpiralStorage::logical_to_physical(uint8_t logical){
    uint8_t high = (1 + logical)/b;
    uint8_t low = logical/b;
    uint8_t physical;
    if (low < high){
        physical = logical_to_physical(low);
    }
    else {
        physical = logical - low; 
    }
    return physical;
}

uint8_t SpiralStorage::table_construction(){
    look_up_table = (uint64_t *) malloc(TABLE_SIZE*sizeof(uint64_t));
    size_of_table = TABLE_SIZE;
    if (look_up_table == NULL) {
        //printf("Memory not allocated.\n");
        exit(0);
    }
    int i;
    for(i = 0; i < size_of_table;i++){
        look_up_table[i] = logical_to_physical(i);
    }
    cout << "Spiral storage look up table initialization succeeds" << endl;
    cout << "The length of table is " << size_of_table << endl;
    return true;
}

uint8_t SpiralStorage::table_reconstruction(){
    look_up_table = (uint64_t *) realloc(look_up_table,(size_of_table + TABLE_SIZE)*sizeof(uint64_t));
    size_of_table += TABLE_SIZE;
    if (look_up_table == NULL) {
        //printf("Memory not allocated.\n");
        exit(0);
    }
    int i;
    for(i = 0; i < size_of_table;i++){
        look_up_table[i] = logical_to_physical(i);
    }
    cout << "Spiral storage look up table initialization succeeds" << endl;
    cout << "the length of table is %d \n" << size_of_table << endl;
    return true;
}

uint8_t SpiralStorage::table_construction_in_PM(){
    size_of_table = TABLE_SIZE;
    _look_up_table =   pmemobj_tx_zalloc(
        sizeof(uint64_t) * TABLE_SIZE, TOID_TYPE_NUM(char));
    look_up_table =
    (uint64_t *)cache_align(pmemobj_direct(_look_up_table));
    pmemobj_tx_add_range_direct(look_up_table,sizeof(uint64_t)*TABLE_SIZE);
    for(int i = 0; i < size_of_table;i++){
        look_up_table[i] = logical_to_physical(i);
    }
    cout << "Spiral storage look up table in PM initialization succeeds" << endl;
    cout << "The length of table is " << size_of_table << endl;
    return true;
}


/*SpiralStorage::SpiralStorage(size_t init_size, PMEMobjpool *_pop) {
   cout << "Initization Begins" << endl;
   TX_BEGIN(_pop){
    pmemobj_tx_add_range_direct(this,sizeof(*this));
   //modify the parameters
    SpiralStorage* spiral = this;
    spiral->pop = _pop;
    spiral->size = init_size;
    spiral->capacity = pow(2,init_size);
    generate_seeds();
    spiral->_buckets = pmemobj_tx_zalloc(
        sizeof(Node) * spiral->capacity, TOID_TYPE_NUM(char));
    spiral->status = 0;
    spiral->item_num = 0;
    spiral->b = 2;
    spiral->c = 2.0;
    spiral->look_up_table = NULL;
    spiral->size_of_table = 0;
    spiral->resizing_lock = 0;
	spiral->locksize = 256;

    #ifdef LOOK_UP_TABLE
    table_construction();
    #endif

    spiral->expand_location = 0;

    spiral->_old_mutex = OID_NULL;

    spiral->buckets =
        (Node *)cache_align(pmemobj_direct(spiral->_buckets));
    spiral->mutex = (PMEMrwlock *)pmemobj_direct(spiral->_mutex);
   }
   TX_END
  cout<<"The number of all buckets:"<< capacity << endl;
  cout<<"The number of all entries:"<< capacity*ASSOC_NUM << endl;
  cout<<"The persistent spiral storage table initialization succeeds!" << endl;
}*/

void SpiralStorage::init_Spiral(PMEMobjpool *_pop, SpiralStorage *spiral,size_t init_size){
    cout << "Initization Begins" << endl;
   TX_BEGIN(_pop){
    pmemobj_tx_add_range_direct(spiral,sizeof(class SpiralStorage));
   //modify the parameters
    spiral->pop = _pop;
    spiral->size = pow(2,init_size);
    spiral->capacity = pow(2,init_size) * ASSOC_NUM;
    generate_seeds();

    spiral->status = 0;
    spiral->item_num = 0;
    spiral->b = 2;
    spiral->c = 0.0;
    spiral->threshold = 0.8;
    spiral->look_up_table = NULL;
    spiral->size_of_table = 0;
    spiral->resizing = false;
    spiral->resizing_lock = 0;
	  spiral->locksize = 256;
	  spiral->nlocks = spiral->capacity / spiral->locksize + 4;
    #ifdef LOOK_UP_TABLE
    table_construction();
    #endif
    spiral->expand_location = 0;

    PMEMoid _buckets = pmemobj_tx_zalloc(
        sizeof(Node*) * spiral->capacity, TOID_TYPE_NUM(char));
    spiral->buckets =
        (Node **)cache_align(pmemobj_direct(_buckets));    

    for(int i = 0 ; i < size; i ++){
		PMEMoid init_node = pmemobj_tx_zalloc(sizeof(Node),TOID_TYPE_NUM(char));
		buckets[i] = (Node *)cache_align(pmemobj_direct(init_node));
    }

	/*allocate*/
    spiral->_mutex = pmemobj_tx_zalloc(sizeof(PMEMrwlock) * spiral->nlocks,
                                      TOID_TYPE_NUM(char));

    /* Intialize pointer*/
    spiral->mutex = (PMEMrwlock *)pmemobj_direct(spiral->_mutex);

    spiral->_old_mutex = OID_NULL;

   }
   TX_END
  cout<<"The number of all buckets: "<< size<< endl;
  cout<<"The number of all entries: "<< capacity << endl;
  cout<<"The persistent spiral storage table initialization succeeds!" << endl;
}

bool SpiralStorage::Insert(Key_t &key, Value_t value){
RETRY:
  while (resizing_lock.load()== 1) {
    asm("nop");
  }
    double f_hash = Transfer_HASH(key);
    uint64_t f_idx = Transfer_IDX(f_hash);
    uint64_t physical;
    //cout<<"f_idx is " << f_idx << endl;
    #ifdef LOOK_UP_TABLE
    if (f_idx < size_of_table){
        physical = look_up_table[f_idx];
    }else{
        cout << "f_idx out of the range of look up table, table expands starts" << endl;
        table_reconstruction();
        cout << "look up table expands correctly,try insertion again" << endl;
        goto RETRY;
        return true;
    }
    #endif
    #ifndef LOOK_UP_TABLE
    physical = logical_to_physical(f_idx);
    #endif
    //cout <<"physical address is " << physical << endl;
    uint64_t i;
    for(i = 0;i < ASSOC_NUM; i ++){
        if (buckets[physical]->token[i] == 0) {
          //cout <<"Bucket Check" << endl;
            while (pmemobj_rwlock_trywrlock(pop, &mutex[physical / locksize]) != 0) {
              if (resizing == true) {
                goto RETRY;
              }
            }

            if (resizing == true) {
              pmemobj_rwlock_unlock(pop, &mutex[physical / locksize]);
              goto RETRY;
            }
          buckets[physical]->slot[i].value = value;
          buckets[physical]->slot[i].key = key;
          pmemobj_persist(pop, &buckets[physical]->slot[i], sizeof(Entry));
          buckets[physical]->token[i] = 1;
          pmemobj_persist(pop, &buckets[physical]->token[i], sizeof(uint8_t));
          //cout << "unlock for lock " << physical / locksize << endl;
          //cout << "insert success" << endl;
          item_num ++;
          pmemobj_persist(pop, &item_num, sizeof(item_num));
          pmemobj_rwlock_unlock(pop, &mutex[physical / locksize]);
          return true;
        }
	  //cout <<"Insert for bucket location " << i <<" fail"<<endl;
    }
    //if insert fails, the table should be expanded
	  //cout << "Bucket is full" << endl;
    if (item_num > capacity * threshold)
    {
      int64_t lock = 0;
      if(resizing_lock.compare_exchange_strong(lock, 1)){
        resize(pop);
        resizing_lock.store(0);
        goto RETRY;
      }
    }
    //cout << "cannot find location" << endl;
    return false;
}

bool SpiralStorage::InsertOnly(Key_t &key, Value_t value){
RETRY:
    double f_hash = Transfer_HASH(key);
    //cout << "f_hash is " << f_hash << endl;
    uint64_t f_idx = Transfer_IDX(f_hash);
    uint64_t physical;
    //cout<<"f_idx is " << f_idx << endl;
    #ifdef LOOK_UP_TABLE
    if (f_idx < size_of_table){
        physical = look_up_table[f_idx];
    }else{
        cout << "f_idx out of the range of look up table, table expands starts" << endl;
        table_reconstruction();
        cout << "look up table expands correctly,try insertion again" << endl;
        Insert(key,value);
        return 0;
    }
    #endif
    #ifndef LOOK_UP_TABLE
    physical = logical_to_physical(f_idx);
    #endif
    //cout <<"physical address is " << physical << endl;
    //cout <<"physical address tested" <<endl;
    uint64_t i;
    for(i = 0;i < ASSOC_NUM; i ++){
		//cout << "Lock happens in " << physical/locksize << endl;
        if (buckets[physical]->token[i] == 0 || i ==  ASSOC_NUM - 1) {
		    //cout <<"Bucket Check" << endl;
        buckets[physical]->slot[i].value = value;
        buckets[physical]->slot[i].key = key;
        pmemobj_persist(pop, &buckets[physical]->slot[i], sizeof(Entry));
        buckets[physical]->token[i] = 1;
        pmemobj_persist(pop, &buckets[physical]->token[i], sizeof(uint8_t));
        return 0;
      }
	  //cout <<"Insert for bucket location " << i <<" fail"<<endl;
    }
    return 1;
}


void SpiralStorage::resize(PMEMobjpool *_pop){
  //cout << "Resizing start" << endl;
  resizing = true;

  bool new_lock = false; 
  for (int i = 0; i < nlocks; ++i) {
    pmemobj_rwlock_wrlock(pop, &mutex[i]);
  }
  
  uint64_t new_size = size + STEP;
  uint64_t new_capacity = capacity + STEP * ASSOC_NUM;

  auto d = buckets;
  auto _dir = new Node * [new_size];
  memcpy(_dir, d, sizeof(Node*)*size);

  auto ret = pmemobj_zalloc(pop,&_new_buckets,sizeof(Node)* STEP, TOID_TYPE_NUM(char));
  new_buckets = (Node *)cache_align(pmemobj_direct(_new_buckets));
  if(ret){
	cout << "Ret is" << ret << endl;
	exit(0);
  }

  //memcpy(_dir[size],&new_buckets[0],sizeof(Node*)*STEP);
  for(int i=0;i < STEP;i++){
     _dir[size+i] = &new_buckets[i];
  }

  c = log2(size + STEP-1);
  pmemobj_persist(pop,&c,sizeof(c));
	buckets = _dir;
  pmemobj_persist(pop,&_dir,sizeof(_dir)*new_size);
  capacity = new_capacity;
	pmemobj_persist(pop,&capacity,sizeof(capacity));
  size = new_size;
	pmemobj_persist(pop,&size,sizeof(size));
  new_buckets = NULL;
  pmemobj_persist(pop,&new_buckets,sizeof(Node*));


  if(new_size / locksize > nlocks){
	cout << "Resizing locks" << endl;
	_old_mutex = _mutex;
	pmemobj_persist(pop, &_old_mutex, sizeof(_old_mutex));
	nlocks = (new_size / locksize) + 1;
  pmemobj_persist(pop, &nlocks, sizeof(nlocks));
	 //cout << "nlocks is " << nlocks <<endl;
	 //cout << "nlocks size is " << nlocks * sizeof(PMEMrwlock) <<endl;
	 //cout << "locks checkpoint 3" << endl;
	auto ret = pmemobj_zalloc(pop, &_mutex, nlocks * sizeof(PMEMrwlock),
								TOID_TYPE_NUM(char));
	 //cout << "locks checkpoint 4" << endl;
	if (ret) {
		 //cout << "Ret = " << ret << endl;
		 //cout << "Allocation Size = " << hex << nlocks * sizeof(PMEMrwlock) << endl;
		LOG_FATAL("Allocation Error in New Mutex");
	}
	mutex = (PMEMrwlock *)(pmemobj_direct(_mutex));
  }

  if(new_lock == false){
    for (int i = 0; i < nlocks; ++i) {
    pmemobj_rwlock_unlock(pop, &mutex[i]);
  }
  }

  resizing = false;
  pmemobj_persist(pop,&resizing,sizeof(resizing));

  //cout << "New c is set to be " << c <<endl;
  //cout << "Current size is " << size <<endl;
  //cout << "Done! Resizing Finsihed for location " << expand_location << endl;
}




Value_t SpiralStorage::Get(Key_t &key){
while (resizing_lock.load() == true) {
    asm("nop");
  }
  RETRY:    
    double f_hash = Transfer_HASH(key);
    uint64_t f_idx = Transfer_IDX(f_hash);
    uint64_t physical;
    #ifdef LOOK_UP_TABLE
    if (f_idx < size_of_table){
        physical = look_up_table[f_idx];
    }else{
      cout << "idx out of range" << endl;
      exit(1);
    }
    //cout << "physical is " << physical << endl;
    #endif
    #ifndef LOOK_UP_TABLE
    physical = logical_to_physical(f_idx);
    #endif
    //printf("physical address is %d \n",physical);

    for (int i = 0; i < ASSOC_NUM; i++) {
          if ((buckets[physical]->token[i] == 1) &&
              buckets[physical]->slot[i].key == key) {
                while (pmemobj_rwlock_tryrdlock(pop, &mutex[physical / locksize]) != 0) {
                      if (resizing == true) {
                        goto RETRY;
                      }
                    }
                if (resizing == true) {
                  pmemobj_rwlock_unlock(pop, &mutex[physical / locksize]);
                  goto RETRY;
                }
                Value_t get_value = buckets[physical]->slot[i].value;
                pmemobj_rwlock_unlock(pop, &mutex[physical / locksize]);
                return get_value;
          }
    }
  return NONE;
}

bool SpiralStorage::Positive_Get(Key_t &key){
while (resizing_lock.load() == true) {
    asm("nop");
  }
  RETRY:    
    double f_hash = Transfer_HASH(key);
    uint64_t f_idx = Transfer_IDX(f_hash);
    uint64_t physical;
    #ifdef LOOK_UP_TABLE
    if (f_idx < size_of_table){
        physical = look_up_table[f_idx];
    }else{
      cout << "idx out of range" << endl;
      exit(1);
    }
    //cout << "physical is " << physical << endl;
    #endif
    #ifndef LOOK_UP_TABLE
    physical = logical_to_physical(f_idx);
    #endif
    //printf("physical address is %d \n",physical);
    for (int i = 0; i < ASSOC_NUM; i++) {
          if ((buckets[physical]->token[i] == 1) &&
              buckets[physical]->slot[i].key != key) {
                while (pmemobj_rwlock_tryrdlock(pop, &mutex[physical / locksize]) != 0) {
                      if (resizing == true) {
                        goto RETRY;
                      }
                    }
                if (resizing == true) {
                  pmemobj_rwlock_unlock(pop, &mutex[physical / locksize]);
                  goto RETRY;
                }
                Value_t get_value = buckets[physical]->slot[i].value;
                pmemobj_rwlock_unlock(pop, &mutex[physical / locksize]);
                return get_value;
          }
    }
  return NONE;
}

bool SpiralStorage::Negative_Get(Key_t &key){
while (resizing_lock.load() == true) {
    asm("nop");
  }
  RETRY:    
    double f_hash = Transfer_HASH(key);
    uint64_t f_idx = Transfer_IDX(f_hash);
    uint64_t physical;
    #ifdef LOOK_UP_TABLE
    if (f_idx < size_of_table){
        physical = look_up_table[f_idx];
    }else{
      cout << "idx out of range" << endl;
      exit(1);
    }
    //cout << "physical is " << physical << endl;
    #endif
    #ifndef LOOK_UP_TABLE
    physical = logical_to_physical(f_idx);
    #endif
    //printf("physical address is %d \n",physical);
    for (int i = 0; i < ASSOC_NUM; i++) {
          if ((buckets[physical]->token[i] == 1) &&
              buckets[physical]->slot[i].key == key &&
              buckets[physical]->slot[i].key != key){
                while (pmemobj_rwlock_tryrdlock(pop, &mutex[physical / locksize]) != 0) {
                      if (resizing == true) {
                        goto RETRY;
                      }
                    }
                if (resizing == true) {
                  pmemobj_rwlock_unlock(pop, &mutex[physical / locksize]);
                  goto RETRY;
                }
                Value_t get_value = buckets[physical]->slot[i].value;
                pmemobj_rwlock_unlock(pop, &mutex[physical / locksize]);
                return true;
          }
    }
  return false;
}


bool SpiralStorage::Delete(Key_t &key){
RETRY:
    while (resizing_lock.load() == true) {
    asm("nop");
  }
  double f_hash = Transfer_HASH(key);
  uint64_t f_idx = Transfer_IDX(f_hash);
  uint64_t physical;

  //cout<<"insert starts"<< endl;
  //cout<<"f_idx is" << f_idx << endl;
  #ifdef LOOK_UP_TABLE
  if (f_idx < size_of_table){
        physical = look_up_table[f_idx];
  }else{
    physical = logical_to_physical(f_idx);
  }
  #endif
  #ifndef LOOK_UP_TABLE
  physical = logical_to_physical(f_idx);
  #endif

  for (int i = 0; i < ASSOC_NUM; i++) {
          if ((buckets[physical]->token[i] == 1) &&
              buckets[physical]->slot[i].key == key) {
            pmemobj_rwlock_trywrlock(pop, &mutex[physical / locksize]);
            buckets[physical]->token[i] = 0;
            pmemobj_persist(pop, &buckets[physical]->token[i], sizeof(uint8_t));
            pmemobj_rwlock_unlock(pop, &mutex[physical / locksize]);
            return true;
          }
    }
  return false;  
}

bool SpiralStorage::Update(Key_t &key, Value_t value){
    while (resizing_lock.load() == true) {
    asm("nop");
  }
  double f_hash = Transfer_HASH(key);
  uint64_t f_idx = Transfer_IDX(f_hash);
  uint64_t physical;

  //cout<<"insert starts"<< endl;
  //cout<<"f_idx is" << f_idx << endl;
  #ifdef LOOK_UP_TABLE
  if (f_idx < size_of_table){
        physical = look_up_table[f_idx];
  }else{
    physical = logical_to_physical(f_idx);
  }
  #endif
  #ifndef LOOK_UP_TABLE
  physical = logical_to_physical(f_idx);
  #endif
    for (int i = 0; i < ASSOC_NUM; i++) {
          if ((buckets[physical]->token[i] == 1) &&
              buckets[physical]->slot[i].key == key) {
            pmemobj_rwlock_trywrlock(pop, &mutex[physical / locksize]);
            buckets[physical]->slot[i].value = value;
            pmemobj_persist(pop, &buckets[physical]->slot[i].value, sizeof(value));
            pmemobj_rwlock_unlock(pop, &mutex[physical / locksize]);
            return true;
          }
    }
  return false;
}


bool SpiralStorage::Recovery(){
  if(resizing != 1 && resizing_lock.load() == true){
    return 0;
  }
  cout << "testing 1" << endl;
  TX_BEGIN(pop){
    resizing = 0;
    resizing_lock.store(0);
    if(size / locksize > nlocks){
      cout << "Resizing locks" << endl;
      _old_mutex = _mutex;
      pmemobj_persist(pop, &_old_mutex, sizeof(_old_mutex));
      nlocks = (size / locksize) + 1;
      pmemobj_persist(pop, &nlocks, sizeof(nlocks));
      //cout << "nlocks is " << nlocks <<endl;
      //cout << "nlocks size is " << nlocks * sizeof(PMEMrwlock) <<endl;
      //cout << "locks checkpoint 3" << endl;
      auto ret = pmemobj_zalloc(pop, &_mutex, nlocks * sizeof(PMEMrwlock),
                    TOID_TYPE_NUM(char));
      //cout << "locks checkpoint 4" << endl;
      if (ret) {
        //cout << "Ret = " << ret << endl;
        //cout << "Allocation Size = " << hex << nlocks * sizeof(PMEMrwlock) << endl;
        LOG_FATAL("Allocation Error in New Mutex");
      }
      mutex = (PMEMrwlock *)(pmemobj_direct(_mutex));
    }
    if(new_buckets != NULL){
      if(pow(b,c) - pow(b,c+1) > size){
        c = log2(size - STEP +1);
        pmemobj_tx_free(_new_buckets);
      }
      new_buckets = NULL;
    }
  }TX_END
  return true;
  cout << "testing ends" << endl;
}

hash_Utilization SpiralStorage::Utilization(){
  struct hash_Utilization h;
  h.load_factor = 0 / size;
  h.utilization = 0;
  return h; 
}


#endif // Spiral_Storage_