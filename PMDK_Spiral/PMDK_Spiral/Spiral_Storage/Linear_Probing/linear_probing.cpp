#include <iostream>
#include <cstring>
#include <thread>
#include <mutex>
#include "linear_probing.h"
#include "time.h"
#include "../../common/hash_api.h"
#include <sys/stat.h>

using namespace std;

TOID_DECLARE(char,16);

static const char *pool_name = "pmem_hash.data";

void *cache_align(void *ptr) {
  uint64_t pp = (uint64_t)ptr;
  pp += 48;
  return (void *)pp;
}

static bool FileExists(const char *pool_path) {
  struct stat buffer;
  if (stat(pool_path, &buffer) == 0){
	remove(pool_name);
  }
  return 0;
}



#define CAS(_p, _u, _v)                                             \
  (__atomic_compare_exchange_n(_p, _u, _v, false, __ATOMIC_ACQUIRE, \
                               __ATOMIC_ACQUIRE))



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
    	pop = pmemobj_create(pool_name, "LinearHashing", 1024 * 1024 * 32UL, 0666);
    	if (pop == NULL) {
			perror(pool_name);
			std::exit(1);
		}
		    //size_t segment_number = (size_t)argv[1];
    size_t init_size = sz >= 2 ? sz : 2;
    PMEMoid root = pmemobj_root(pop, sizeof (LinearProbingHash));
		struct LinearProbingHash *rootp = (LinearProbingHash*)pmemobj_direct(root);
    new (rootp) LinearProbingHash();
		rootp->init_Linear(pop,rootp,init_size);
		if (rootp == NULL)
		{
			perror("Pointer error");
			std::exit(1);
		}
  std::cout << "Return the pointer to Linear Hashing" << std::endl;
  return rootp;
  }
  std::cout << "error" << std::endl;
  return NULL;
}


LinearProbingHash::LinearProbingHash(void){}

LinearProbingHash::LinearProbingHash(size_t _capacity)
  : capacity{_capacity}, dict{new Pair[capacity]}
{
  locksize = 256;
  nlocks = (capacity)/locksize+1;
}

LinearProbingHash::~LinearProbingHash(void) {
  if (dict != nullptr) delete[] dict;
}

void LinearProbingHash::init_Linear(PMEMobjpool *_pop, LinearProbingHash *linear,size_t init_size){
  TX_BEGIN(_pop){
    pmemobj_tx_add_range_direct(this,sizeof(*this));
   //modify the parameters
    LinearProbingHash* linear = this;
    std::cout << "init begins" <<std::endl;
    linear->pop = _pop;
    linear->locksize = 256;
    linear->capacity = init_size;
    linear->nlocks = (linear->capacity)/linear->locksize+1;
    linear->_dict = pmemobj_tx_zalloc(
        sizeof(Pair) * linear->capacity, TOID_TYPE_NUM(char));
    linear->dict =
        (Pair *)cache_align(pmemobj_direct(linear->_dict));
    for (int i = 0; i < linear->capacity; i++)
    {
      linear->dict[i].key = INVALID;
    }
    
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


bool LinearProbingHash::Insert(Key_t& key, Value_t value) {
  using namespace std;
  auto key_hash = h(&key, sizeof(key));
  std::cout <<"Insert starts" <<std::endl;
RETRY:
  while (resizing_lock == 1) {
    asm("nop");
  }
  auto loc = key_hash % capacity;
  if (size < capacity * kResizingThreshold) {
    auto i = 0;
    while (i < capacity) {
      auto slot = (loc + i) % capacity;
      //cout <<"slot key is" << dict[slot].key<< endl;
      pmemobj_rwlock_trywrlock(pop,&mutex[slot/locksize]);
      do {
        if (dict[slot].key == INVALID) {
          dict[slot].value = value;
          dict[slot].key = key;
          pmemobj_persist(pop, &dict[slot].value, sizeof(value));
          pmemobj_persist(pop, &dict[slot].key, sizeof(key));
          //pmemobj_persist(pop, &dict[slot], sizeof(Pair));
          auto _size = size;
          while (!CAS(&size, &_size, _size+1)) {
            _size = size;
          }
          pmemobj_persist(pop, &size, sizeof(size));
          pmemobj_rwlock_unlock(pop,&mutex[slot/locksize]);
          return true;
        }
        i++;
        slot = (loc + i) % capacity;
        if (!(i < capacity)) break;
      } while (slot % locksize != 0);
    }
  } else {
    resizing_lock = 1;
    pmemobj_persist(pop, &resizing_lock, sizeof(resizing_lock));
    resize(capacity * kResizingFactor);
    resizing_lock = 0;
    pmemobj_persist(pop, &resizing_lock, sizeof(resizing_lock));
  }
  goto RETRY;
  return false;
}

bool LinearProbingHash::InsertOnly(Key_t& key, Value_t value) {
  auto key_hash = h(&key, sizeof(key)) % capacity;
  auto loc = getLocation(key_hash, capacity, dict);
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

bool LinearProbingHash::Delete(Key_t& key) {
    auto key_hash = h(&key, sizeof(key)) % capacity;
  for (size_t i = 0; i < capacity; ++i) {
    auto id = (key_hash + i) % capacity;
    pmemobj_rwlock_trywrlock(pop,&mutex[id/locksize]);
    if (dict[id].key == INVALID) return NONE;
    if (dict[id].key == key) {
      dict[id].key = INVALID;
      pmemobj_persist(pop, &dict[id].key, sizeof(key));
      return true;
    }
  }
  return NONE;
}

Value_t LinearProbingHash::Get(Key_t& key) {
  auto key_hash = h(&key, sizeof(key)) % capacity;
  for (size_t i = 0; i < capacity; ++i) {
    auto id = (key_hash + i) % capacity;
    pmemobj_rwlock_trywrlock(pop,&mutex[id/locksize]);
    if (dict[id].key == INVALID) return NONE;
    if (dict[id].key == key) return std::move(dict[id].value);
  }
  return NONE;
}

double LinearProbingHash::Utilization(void) {
  size_t size = 0;
  for (size_t i = 0; i < capacity; ++i) {
    if (dict[i].key != 0) {
      ++size;
    }
  }
  return ((double)size)/((double)capacity)*100;
}

size_t LinearProbingHash::getLocation(size_t hash_value, size_t _capacity, Pair* _dict) {
  Key_t LOCK = INVALID;
  size_t cur = hash_value;
  size_t i = 0;
FAILED:
  while (_dict[cur].key != INVALID) {
    cur = (cur + 1) % _capacity;
    ++i;
    if (!(i < capacity)) {
      return INVALID;
    }
  }
  if (CAS(&_dict[cur].key, &LOCK, SENTINEL)) {
    return cur;
  } else {
    goto FAILED;
  }
}

void LinearProbingHash::resize(size_t _capacity) {
  cout << "resize start for new capacity " << _capacity << endl;
  timer.Start();
  for (int i = 0; i < nlocks; ++i) {
    pmemobj_rwlock_wrlock(pop, &mutex[i]);
  }
  int prev_nlocks = nlocks;
  nlocks = _capacity/locksize+1;
  pmemobj_persist(pop, &nlocks, sizeof(nlocks));

  _old_mutex = _mutex;

  pmemobj_persist(pop, &_old_mutex, sizeof(_old_mutex));

  PMEMoid _newDict;

  auto ret = pmemobj_zalloc(pop,&_newDict,_capacity * sizeof(Pair),TOID_TYPE_NUM(char));

  Pair * newDict = (Pair *)cache_align(pmemobj_direct(_newDict));   

  for (int i = 0; i < _capacity; i++) {
    newDict[i].key = INVALID;
    newDict[i].value = 0;
    pmemobj_persist(pop, &newDict[i], sizeof(Pair));
  }

    for (int i = 0; i < capacity; i++) {
      if (dict[i].key != INVALID) {
        auto key_hash = h(&dict[i].key, sizeof(Key_t)) % _capacity;
        auto loc = getLocation(key_hash, _capacity, newDict);
        newDict[loc].key = dict[i].key;
        newDict[loc].value = dict[i].value;
        //pmemobj_persist(pop, &newDict[loc].value, sizeof(Value_t));
        //pmemobj_persist(pop, &newDict[loc].key, sizeof(Key_t));
        pmemobj_persist(pop, &newDict[loc], sizeof(Pair));
      }
    }

    //pmemobj_persist(pop,&newDict[0],sizeof(Pair)*_capacity);  

    ret = pmemobj_zalloc(pop,&_mutex,nlocks * sizeof(PMEMrwlock), TOID_TYPE_NUM(char));

    mutex = (PMEMrwlock *)(pmemobj_direct(_mutex));
    pmemobj_persist(pop,&mutex,sizeof(mutex));

    old_cap = capacity;
    pmemobj_persist(pop,(char*)&old_cap, sizeof(old_cap));
    old_dic = dict;
    pmemobj_persist(pop,(char*)&old_dic, sizeof(Pair*));
    _old_dic = _dict;
    pmemobj_persist(pop,(char*)&_old_dic, sizeof(_old_dic));
    dict = newDict;

    capacity = _capacity;
    pmemobj_persist(pop,(char*)&capacity, sizeof(size_t));
    old_cap = 0;
    pmemobj_persist(pop,(char*)&old_cap, sizeof(size_t));
    old_dic = NULL;
    pmemobj_persist(pop,(char*)&old_dic, sizeof(Pair*));
    
  TX_BEGIN(pop){
    pmemobj_tx_add_range_direct(this, sizeof(class LinearProbingHash));
    pmemobj_tx_free(_old_dic);
    pmemobj_tx_free(_old_mutex);
    pmemobj_tx_free(_newDict);
  }
  TX_ONABORT {printf("resizing txn 2 fails\n"); }
  TX_END
  timer.Stop();
  breakdown += timer.GetSeconds();
}



//header ends, now main begins


int main(int argc, char *argv[]){
  // Step 1: create (if not exist) and open the pool
	  std::cout << "Testing Begin" <<std::endl;
    bool file_exist = false;
    if (FileExists(pool_name)) file_exist = true;

  if (!file_exist){
		PMEMobjpool *pop;
    	pop = pmemobj_create(pool_name, "LinearHashing", 1024 * 1024 * 32UL, 0666);
    	if (pop == NULL) {
			perror(pool_name);
			std::exit(1);
		}
		    //size_t segment_number = (size_t)argv[1];
    size_t init_size = 2;
    PMEMoid root = pmemobj_root(pop, sizeof (LinearProbingHash));
		struct LinearProbingHash *rootp = (LinearProbingHash*)pmemobj_direct(root);
    new (rootp) LinearProbingHash();
    std::cout << "new ends" <<std::endl;
		rootp->init_Linear(pop,rootp,init_size);
		if (rootp == NULL)
		{
			perror("Pointer error");
			std::exit(1);
		}
		//rootp->Show_Look_Up_Table();
		int already_exists = 0;
		for (uint64_t i = 0; i < 1; ++i) {
    	// Enroll into the epoch, if using one thread, epoch mechanism is actually
			for (uint64_t j = 0; j < 20; ++j) {
			uint64_t Key = i * 1024 + j;
			auto ret = rootp->Insert(Key, "1");
			std::cout << "Insert Ends for key " << i * 1024 + j<< std::endl;
			if (ret == -1) already_exists++;
		}
    }
	pmemobj_close(pop);	
	}
	std::cout << "testing ends" <<std::endl;
	// Close PMEM object pool
		
	return 0;
}