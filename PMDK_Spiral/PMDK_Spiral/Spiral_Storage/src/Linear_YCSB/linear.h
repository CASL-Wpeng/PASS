#include <iostream>
#include <cstring>
#include <thread>
#include <mutex>
#include "time.h"
#include "../../common/hash_api.h"
#include <sys/stat.h>
#include <libpmemobj.h>
#include "../../common/timer.h"
#include "../../common/hash.h"
#include <math.h>
#include <atomic>
using namespace std;

#ifndef LINEAR_HASH_H_
#define LINEAR_HASH_H_

TOID_DECLARE(char,16);


typedef size_t Key_t;
typedef const char* Value_t;

const Key_t SENTINEL = -2;  // 11111...110
const Key_t INVALID = -1;   // 11111...111

const Value_t NONE = 0x0;

#define KEY_LEN 8
#define VALUE_LEN 8


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

#define ASSOC_NUM 16

struct Node
{
  Entry slot[ASSOC_NUM];
};

void *cache_align(void *ptr) {
  uint64_t pp = (uint64_t)ptr;
  pp += 48;
  return (void *)pp;
}

#define CAS(_p, _u, _v)                                             \
  (__atomic_compare_exchange_n(_p, _u, _v, false, __ATOMIC_ACQUIRE, \
                               __ATOMIC_ACQUIRE))


class LinearHash : public hash_api {
  const float kResizingFactor = 2;
  const float kResizingThreshold = 0.75;
  public:
    PMEMobjpool *pop;
    LinearHash(void);
    LinearHash(size_t);
    ~LinearHash(void);
    void init_Linear(PMEMobjpool *_pool, LinearHash *linear,size_t init_size);
    bool Insert(Key_t&, Value_t);
    bool InsertOnly(Key_t&, Value_t);
    bool Delete(Key_t&);
    bool Update(Key_t&, Value_t);
    Value_t Get(Key_t&);
    double Utilization(void);
    size_t Capacity(void) {
      return capacity;
    }
    std::string hash_name() { return "Linear"; };

  bool find(const char *key, size_t key_sz, char *value_out, unsigned tid)
  {
    Key_t k = *reinterpret_cast<const Key_t *>(key);
    //cout << "read start" << endl;
    auto r = Get(k);
    //cout << "read success" << endl;
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
    Update(k, value);
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
  PMEMoid _dict;
  Node** dict;
  size_t capacity;
  size_t size;
  void resize(size_t);
  size_t getLocation(size_t, size_t, Node*);

  size_t old_cap;
  Node** new_dic;
  PMEMoid _new_dic;
  uint64_t item_number = 0;

  atomic<int64_t> resizing_lock;
  bool resizing;
  uint64_t nlocks;
  uint64_t locksize;
  uint64_t resize_loc;
  uint64_t hash_factor;
  uint64_t thread_num;
  Timer timer;
  double breakdown = 0;
  PMEMoid _mutex;
  PMEMoid _old_mutex; /*old mutex lock*/
  PMEMrwlock *mutex;
};


LinearHash::LinearHash(void){}


LinearHash::~LinearHash(void) {
  if (dict != nullptr) delete[] dict;
}


void LinearHash::init_Linear(PMEMobjpool *_pop, LinearHash *linear,size_t init_size){
  TX_BEGIN(_pop){
    pmemobj_tx_add_range_direct(this,sizeof(*this));
   //modify the parameters
    LinearHash* linear = this;
    std::cout << "init begins" <<std::endl;
    linear->pop = _pop;
    linear->locksize = 256;
    linear->size = init_size;
    linear->capacity = init_size * ASSOC_NUM;
    linear->nlocks = (linear->size)/linear->locksize+1;
    PMEMoid _buckets = pmemobj_tx_zalloc(
        sizeof(Node*) * linear->capacity, TOID_TYPE_NUM(char));
    linear->dict =
        (Node **)cache_align(pmemobj_direct(_buckets));  

    for(int i = 0 ; i < linear->capacity ; i ++){
		  PMEMoid init_node = pmemobj_tx_zalloc(sizeof(Node),TOID_TYPE_NUM(char));
		  dict[i] = (Node *)cache_align(pmemobj_direct(init_node));
    }
    linear->hash_factor = 2;
    linear->resize_loc = 0;
    for (int i = 0; i < linear->size; i++)
    {
      for(int j = 0; j < ASSOC_NUM;j++){
        dict[i]->slot[j].key = INVALID;
      }
    }
    linear->item_number = 0;
    linear->resizing = 0;
    linear->resizing_lock = 0;
    linear->_mutex = pmemobj_tx_zalloc(sizeof(PMEMrwlock) * linear->nlocks,
                                      TOID_TYPE_NUM(char));

    /* Intialize pointer*/
    linear->mutex = (PMEMrwlock *)pmemobj_direct(linear->_mutex);
    linear->new_dic = NULL;
    linear->_new_dic = OID_NULL;
    linear->_old_mutex = OID_NULL;
  }TX_END
  std::cout <<"initlization ends, works correctly" <<std::endl;
}


bool LinearHash::Insert(Key_t& key, Value_t value) {
RETRY:
  auto key_hash = h(&key, sizeof(key));
  //std::cout <<"Insert starts" <<std::endl;
  while (resizing_lock.load() == 1) {
    asm("nop");
  }
  auto loc = key_hash % hash_factor;
  //cout << "key_hash is " << key_hash << endl;
  //cout << "loc is " << loc << endl;
  if(loc >= size)
    loc = loc - hash_factor/2;
  //cout <<"loc for key " << key  << " is "<< loc <<endl;
  cout << "testing insert" << endl;
  for(int i =0; i < ASSOC_NUM; ++i){
      if (dict[loc]->slot[i].key == INVALID) {

  while (pmemobj_rwlock_trywrlock(pop, &mutex[loc / locksize]) != 0) {
        if (resizing == true) {
          goto RETRY;
        }
      }

      if (resizing == true) {
        pmemobj_rwlock_unlock(pop, &mutex[loc / locksize]);
        goto RETRY;
      }
          dict[loc]->slot[i].value = value;
          dict[loc]->slot[i].key = key;
          pmemobj_persist(pop, &dict[loc]->slot[i].value, sizeof(value));
          pmemobj_persist(pop, &dict[loc]->slot[i].key, sizeof(key));
          //pmemobj_persist(pop, &dict[loc]->slot, sizeof(Entry));
          item_number ++;
          pmemobj_persist(pop, &item_number, sizeof(item_number));
          pmemobj_rwlock_unlock(pop,&mutex[loc / locksize]);
          //cout <<"insert success" <<endl;
          return true;
      }
  }
  if(item_number >= capacity * kResizingThreshold){
    int64_t lock = 0;
    if(resizing_lock.compare_exchange_strong(lock, 1)){
      cout << "testing resizing" << endl;
      resize(size+1);
      resizing_lock.store(0);
      goto RETRY;
    }
  }
  //cout << "cannot insert,";
  //cout << " size is " << size;
  //cout << " capacity is " << capacity << endl;
  return false;
}

bool LinearHash::InsertOnly(Key_t& key, Value_t value) {
  auto loc = h(&key, sizeof(key)) % hash_factor;
  for(int i = 0;i < ASSOC_NUM; i++){
    if(dict[loc]->slot[i].key!= INVALID){
    dict[loc]->slot[i].value = value;
    dict[loc]->slot[i].key = key;
    pmemobj_persist(pop, &dict[loc]->slot[i].value, sizeof(value));
    pmemobj_persist(pop, &dict[loc]->slot[i].key, sizeof(key));
    size++;
    return true;
    }
  }
  return false;
}

bool LinearHash::Delete(Key_t& key) {
  while (resizing_lock.load() == 1) {
    asm("nop");
  }
  auto key_hash = h(&key, sizeof(key)) % hash_factor;
    if(key_hash >= size)
      key_hash = key_hash - hash_factor/2;
    for(int i=0; i < ASSOC_NUM; i++){
      if (dict[key_hash]->slot[i].key == key) {
        pmemobj_rwlock_trywrlock(pop,&mutex[key_hash/locksize]);
        dict[key_hash]->slot[i].key = INVALID;
        pmemobj_persist(pop, &dict[key_hash]->slot[i].key, sizeof(key));
        pmemobj_rwlock_unlock(pop,&mutex[key_hash/locksize]);
        return 1;
      }
    }
  return 0;
}

Value_t LinearHash::Get(Key_t& key) {
RETRY:
  while (resizing_lock.load() == 1) {
    asm("nop");
  }
  auto key_hash = h(&key, sizeof(key)) % hash_factor;
  if(key_hash >= size)
      key_hash = key_hash - hash_factor/2;

  //cout << "testing read" << endl;
  while (pmemobj_rwlock_tryrdlock(pop, &mutex[key_hash / locksize]) != 0) {
        if (resizing == true) {
          goto RETRY;
        }
      }

      if (resizing == true) {
        pmemobj_rwlock_unlock(pop, &mutex[key_hash / locksize]);
        goto RETRY;
      }

  for (int i = 0; i < ASSOC_NUM; i++)
  {
    if (dict[key_hash]->slot[i].key == key){
      //cout << "find key " << key << " location is " << key_hash << " hash factor is " << hash_factor << endl;
      Value_t get_value = dict[key_hash]->slot[i].value;
      pmemobj_rwlock_unlock(pop, &mutex[key_hash / locksize]);
      return get_value;
    }
  }
  pmemobj_rwlock_unlock(pop, &mutex[key_hash / locksize]);
  return NONE;
}



bool LinearHash::Update(Key_t& key,Value_t value) {
RETRY:
  while (resizing_lock.load() == 1) {
    asm("nop");
  }
  auto key_hash = h(&key, sizeof(key)) % hash_factor;
  if(key_hash >= size)
      key_hash = key_hash - hash_factor/2;

  //cout << "testing read" << endl;
  while (pmemobj_rwlock_tryrdlock(pop, &mutex[key_hash / locksize]) != 0) {
        if (resizing == true) {
          goto RETRY;
        }
      }

      if (resizing == true) {
        pmemobj_rwlock_unlock(pop, &mutex[key_hash / locksize]);
        goto RETRY;
      }

  for (int i = 0; i < ASSOC_NUM; i++)
  {
    if (dict[key_hash]->slot[i].key == key){
      //cout << "find key " << key << " location is " << key_hash << " hash factor is " << hash_factor << endl;
      dict[key_hash]->slot[i].value = value;
      pmemobj_persist(pop,dict[key_hash]->slot[i].value,sizeof(Value_t));
      pmemobj_rwlock_unlock(pop, &mutex[key_hash / locksize]);
      return true;
    }
  }
  pmemobj_rwlock_unlock(pop, &mutex[key_hash / locksize]);
  return false;
}

double LinearHash::Utilization(void) {
  size_t size = 0;
  for (int i = 0; i < capacity; ++i) {
    for (int j = 0; i < ASSOC_NUM; i++)
    {
    if (dict[i]->slot[j].key != 0) {
      ++size;
    }
    }
  }
  return ((double)size)/((double)capacity)*100;
}

size_t LinearHash::getLocation(size_t hash_value, size_t _capacity, Node* _dict) {
  return hash_value % hash_factor;
}


void LinearHash::resize(size_t _size) {
  //cout << "resize start for new capacity " << _size << endl;
  //cout << "the current hash factor is " << hash_factor<<endl;
  resizing = true;
  pmemobj_persist(pop, &resizing, sizeof(resizing));

  size_t _capacity = _size * ASSOC_NUM;
  bool new_lock = false; 
  for (int i = 0; i < nlocks; ++i) {
    pmemobj_rwlock_wrlock(pop, &mutex[i]);
  }
  int prev_nlocks = nlocks;
  nlocks = _size/locksize+1;
  pmemobj_persist(pop, &nlocks, sizeof(nlocks));

  _old_mutex = _mutex;
  pmemobj_persist(pop, &_old_mutex, sizeof(_old_mutex));
  
  auto ret = pmemobj_zalloc(pop,&_mutex,nlocks * sizeof(PMEMrwlock), TOID_TYPE_NUM(char));

	if (ret) {
		 //cout << "Ret = " << ret << endl;
		 //cout << "Allocation Size = " << hex << nlocks * sizeof(PMEMrwlock) << endl;
		cout << "Allocation Error in New Mutex" << endl;
    exit(0);
	}

  mutex = (PMEMrwlock *)(pmemobj_direct(_mutex));
  pmemobj_persist(pop,&mutex,sizeof(mutex));
  //cout << "test for expanding the lock" << endl;

  TX_BEGIN(pop) {
    pmemobj_tx_add_range_direct(this, sizeof(class LinearHash));
    PMEMoid _new_bucket = pmemobj_tx_zalloc(sizeof(Node), TOID_TYPE_NUM(char));
    Node * new_bucket = (Node *)cache_align(pmemobj_direct(_new_bucket));
	auto d = dict;
	auto _dir = new Node * [_size];
	memcpy(_dir, d, sizeof(Node*)*_size);
	pmemobj_tx_add_range_direct(new_bucket,sizeof(Node));

  for (int i = 0; i < ASSOC_NUM; i++)
  {
    new_bucket->slot[i].key =INVALID;
	  new_bucket->slot[i].value = NONE;
  }
  
  _dir[size] = new_bucket;

	if(_size > hash_factor){
		hash_factor = hash_factor * 2;
		resize_loc = 0;
	}

	//cout << "testing" << endl;
  pmemobj_tx_add_range_direct(_dir[resize_loc],sizeof(Node));
  pmemobj_tx_add_range_direct(_dir[size],sizeof(Node));
  for(int i = 0; i < ASSOC_NUM;i++){
    if(_dir[resize_loc]->slot[i].key != INVALID){
      auto loc = h(&_dir[resize_loc]->slot[i].key, sizeof(Key_t)) % hash_factor;
      if(loc >= _size){
        loc = loc - hash_factor / 2 ;
        cout << "loc out of range, it should be " << endl;
      } 
      if(loc != resize_loc){
        //cout << resize_loc << " item " << i << " resize to " << loc << endl;
        _dir[loc]->slot[i].key = _dir[resize_loc]->slot[i].key;
				_dir[loc]->slot[i].value = _dir[resize_loc]->slot[i].value;
				_dir[resize_loc]->slot[i].key = INVALID;
      }
    }
  }

  pmemobj_tx_free(_old_mutex);
  size = _size;
	capacity = _capacity;
	old_cap = 0;
	resize_loc++;
	dict = _dir;
  resizing = false;
  }TX_END


  //print_all();
  //cout << hash_factor << endl;
  //cout << "current size is " << size << endl;
  //cout << resizing_lock << endl;
}

typedef struct thread_queue{
    Key_t key;
    uint8_t operation;                    // 0: read, 1: insert;
} thread_queue;

typedef struct sub_thread{
    pthread_t thread;
    uint32_t id;
    uint64_t inserted;
    LinearHash *linear;
    thread_queue* run_queue;
} sub_thread;

#endif  // LINEAR_HASH_H_
