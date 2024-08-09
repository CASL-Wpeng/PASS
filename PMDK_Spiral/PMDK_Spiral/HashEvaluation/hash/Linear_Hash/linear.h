#include <iostream>
#include <cstring>
#include <thread>
#include <mutex>
#include "time.h"
#include "../common/hash_api.h"
#include <sys/stat.h>
#include <libpmemobj.h>
#include "../common/pair.h"
#include "../common/timer.h"
#include "../common/hash.h"

using namespace std;


#ifndef LINEAR_HASH_H_
#define LINEAR_HASH_H_

TOID_DECLARE(char,16);


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
  const float kResizingThreshold = 0.95;
  public:
    PMEMobjpool *pop;
    LinearHash(void);
    LinearHash(size_t);
    ~LinearHash(void);
    void init_Linear(PMEMobjpool *_pool, LinearHash *linear,size_t init_size);
    bool Insert(Key_t&, Value_t);
    bool InsertOnly(Key_t&, Value_t);
    bool Delete(Key_t&);
    Value_t Get(Key_t&);
    double Utilization(void);
    size_t Capacity(void) {
      return capacity;
    }
    std::string hash_name() { return "Linear"; };

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
  PMEMoid _dict;
  size_t capacity;
  Pair* dict;
  void resize(size_t);
  size_t getLocation(size_t, size_t, Pair*);

  size_t old_cap;
  Pair* old_dic;
  PMEMoid _old_dic;
  size_t size = 0;

  int resizing_lock = 0;
  int nlocks;
  int locksize;
  int hash_factor;
  int resize_loc;
  Timer timer;
  double breakdown = 0;
  double threshold = 0.8;
  PMEMoid _mutex;
  PMEMoid _old_mutex; /*old mutex lock*/
  PMEMrwlock *mutex;

};


LinearHash::LinearHash(void){}

LinearHash::LinearHash(size_t _capacity)
  : capacity{_capacity}, dict{new Pair[capacity]}
{
  locksize = 256;
  nlocks = (capacity)/locksize+1;
}

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
    linear->capacity = init_size;
    linear->nlocks = (linear->capacity)/linear->locksize+1;
    linear->_dict = pmemobj_tx_zalloc(
        sizeof(Pair) * linear->capacity, TOID_TYPE_NUM(char));
    linear->dict =
        (Pair *)cache_align(pmemobj_direct(linear->_dict));
    linear->hash_factor = 2;
    linear->resize_loc = 0;
    for (int i = 0; i < linear->capacity; i++)
    {
      linear->dict[i].key = INVALID;
    }
    linear->size = 0;
    linear->_mutex = pmemobj_tx_zalloc(sizeof(PMEMrwlock) * linear->nlocks,
                                      TOID_TYPE_NUM(char));

    /* Intialize pointer*/
    linear->mutex = (PMEMrwlock *)pmemobj_direct(linear->_mutex);
    linear->old_dic = NULL;
    linear->_old_dic = OID_NULL;

    linear->_old_mutex = OID_NULL;
  }TX_END
  std::cout <<"initlization ends, works correctly" <<std::endl;
}


bool LinearHash::Insert(Key_t& key, Value_t value) {
  using namespace std;
  auto key_hash = h(&key, sizeof(key));
  //std::cout <<"Insert starts" <<std::endl;
RETRY:
  while (resizing_lock == 1) {
    asm("nop");
  }
  auto loc = key_hash % hash_factor;
  if(loc > capacity)
    loc = loc/2;
  //cout <<"loc is " << loc <<endl;
  if (dict[loc].key == INVALID) {
          pmemobj_rwlock_trywrlock(pop,&mutex[loc/locksize]);
          dict[loc].value = value;
          dict[loc].key = key;
          pmemobj_persist(pop, &dict[loc].value, sizeof(value));
          pmemobj_persist(pop, &dict[loc].key, sizeof(key));
          //pmemobj_persist(pop, &dict[slot], sizeof(Pair));
          auto _size = size;
          while (!CAS(&size, &_size, _size+1)) {
            _size = size;
          }
          pmemobj_persist(pop, &size, sizeof(size));
          pmemobj_rwlock_unlock(pop,&mutex[loc/locksize]);
          return true;
          }
 else if(size > capacity * threshold){
    resizing_lock = 1;
    pmemobj_persist(pop, &resizing_lock, sizeof(resizing_lock));
    resize(capacity+1);
    resizing_lock = 0;
    pmemobj_persist(pop, &resizing_lock, sizeof(resizing_lock));
    goto RETRY;
  }
  return false;
}

bool LinearHash::InsertOnly(Key_t& key, Value_t value) {
  auto loc = h(&key, sizeof(key)) % hash_factor;
  if (loc == INVALID) {
    return false;
  } else {
    dict[loc].value = value;
    dict[loc].key = key;
    pmemobj_persist(pop, &dict[loc].value, sizeof(value));
    pmemobj_persist(pop, &dict[loc].key, sizeof(key));
    size++;
    return true;
  }
}

bool LinearHash::Delete(Key_t& key) {
  auto key_hash = h(&key, sizeof(key)) % hash_factor;
    pmemobj_rwlock_trywrlock(pop,&mutex[key_hash/locksize]);
    if (dict[key_hash].key == INVALID) return 0;
    if (dict[key_hash].key == key) {
      dict[key_hash].key = INVALID;
      pmemobj_persist(pop, &dict[key_hash].key, sizeof(key));
      return 1;
    }
  return 0;
}

Value_t LinearHash::Get(Key_t& key) {
  auto key_hash = h(&key, sizeof(key)) % hash_factor;
  if (dict[key_hash].key == INVALID) return NONE;
  if (dict[key_hash].key == key) return std::move(dict[key_hash].value);
  else return NONE;
}

double LinearHash::Utilization(void) {
  size_t size = 0;
  for (size_t i = 0; i < capacity; ++i) {
    if (dict[i].key != 0) {
      ++size;
    }
  }
  return ((double)size)/((double)capacity)*100;
}

size_t LinearHash::getLocation(size_t hash_value, size_t _capacity, Pair* _dict) {
  return hash_value % hash_factor;
}

void LinearHash::resize(size_t _capacity) {
  //cout << "resize start for new capacity " << _capacity << endl;
  //cout << "the current hash factor is " << hash_factor<<endl;
  timer.Start();
  for (int i = 0; i < nlocks; ++i) {
    pmemobj_rwlock_wrlock(pop, &mutex[i]);
  }
  int prev_nlocks = nlocks;
  nlocks = _capacity/locksize+1;
  pmemobj_persist(pop, &nlocks, sizeof(nlocks));

  _old_mutex = _mutex;

  pmemobj_persist(pop, &_old_mutex, sizeof(_old_mutex));

  TX_BEGIN(pop) {
    pmemobj_tx_add_range_direct(this, sizeof(class LinearHash));
    // Need to be filled to move some k-v pair to the new allocate bucket
    auto ret = pmemobj_realloc(pop, &this->_dict, sizeof(Pair) * _capacity, TOID_TYPE_NUM(char));
    dict = (Pair *)cache_align(pmemobj_direct(this->_dict));;
	if (ret) {
		cout << "Ret = " << ret << endl;
    exit(0);
	  }
  }TX_END

  if(_capacity <= hash_factor){
    dict[_capacity].key = dict[resize_loc].key;
    dict[_capacity].value = dict[resize_loc].value;
    pmemobj_persist(pop, &dict[_capacity], sizeof(Pair));
    resize_loc++;
    pmemobj_persist(pop, &resize_loc, sizeof(resize_loc));
  }else{
    hash_factor = hash_factor*2;
    pmemobj_persist(pop,&hash_factor,sizeof(hash_factor));

    old_dic = new Pair[_capacity];
    for (int i = 0; i < _capacity; i++){
      auto loc = h(&dict[i].key, sizeof(Key_t)) % hash_factor;
      old_dic[i].key = dict[loc].key;
      old_dic[i].value = dict[loc].value;
    }
    for (int i = 0; i < _capacity; i++) {
        dict[i].key = old_dic[i].key;
        if (dict[i].key ==0){
          dict[i].key = INVALID;
        }
        dict[i].value = old_dic[i].value;
        pmemobj_persist(pop, &dict[i], sizeof(Pair));
    }
  }
  auto ret = pmemobj_zalloc(pop,&_mutex,nlocks * sizeof(PMEMrwlock), TOID_TYPE_NUM(char));

  mutex = (PMEMrwlock *)(pmemobj_direct(_mutex));
  pmemobj_persist(pop,&mutex,sizeof(mutex));

  old_cap = capacity;
  pmemobj_persist(pop,(char*)&old_cap, sizeof(old_cap));
  old_dic = dict;
  pmemobj_persist(pop,(char*)&old_dic, sizeof(Pair*));
  _old_dic = _dict;
  pmemobj_persist(pop,(char*)&_old_dic, sizeof(_old_dic));

  capacity = _capacity;
  pmemobj_persist(pop,(char*)&capacity, sizeof(size_t));
  old_cap = 0;
  pmemobj_persist(pop,(char*)&old_cap, sizeof(size_t));
  old_dic = NULL;
  pmemobj_persist(pop,(char*)&old_dic, sizeof(Pair*));

  resize_loc = 0;
  
  pmemobj_persist(pop, &resize_loc, sizeof(resize_loc));
    
  TX_BEGIN(pop){
    pmemobj_tx_add_range_direct(this, sizeof(class LinearHash));
    pmemobj_tx_free(_old_dic);
    pmemobj_tx_free(_old_mutex);
  }
  TX_ONABORT {printf("resizing txn 2 fails\n"); }
  TX_END
  timer.Stop();
  breakdown += timer.GetSeconds();
}



#endif  // LINEAR_HASH_H_
