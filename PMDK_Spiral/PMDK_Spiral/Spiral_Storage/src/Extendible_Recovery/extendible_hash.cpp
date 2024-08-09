#include <iostream>
#include <cmath>
#include <thread>
#include <bitset>
#include <cassert>
#include <unordered_map>
#include "extendible_hash.h"
#include <sys/stat.h>
size_t lockCount = 0;
size_t splitCount = 0;


static const char *pool_name = "pmem_hash.data";
//static const char *pool_name = "../../../../mnt/pmem0/wpeng/pmem_extendible_hash.data";

static const size_t pool_size = 1024 * 1024 * 32UL;

#define READ_WRITE_NUM 50

static bool FileExists(const char *pool_path) {
  struct stat buffer;
  if (stat(pool_path, &buffer) == 0){
    remove(pool_name);
    //return 1;
  }
  return 0;
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
    	pop = pmemobj_create(pool_name, "ExtendibleHash", pool_size, 0666);
    	if (pop == NULL) {
			perror(pool_name);
			exit(1);
		}
		    //size_t segment_number = (size_t)argv[1];
    size_t init_size = sz >= 2 ? sz : 2;
    init_size = 2;
    PMEMoid root = pmemobj_root(pop, sizeof (ExtendibleHash));
		struct ExtendibleHash *rootp = (ExtendibleHash*)pmemobj_direct(root);
    new (rootp) ExtendibleHash();
		rootp->init_Extend(pop,rootp,init_size);
		if (rootp == NULL)
		{
			perror("Pointer error");
			exit(1);
		}
  cout << "Write return the pointer to Extendible Hashing" << std::endl;
  return rootp;
  }else{
    PMEMobjpool *pop;
    pop = pmemobj_open(pool_name, "ExtendibleHash");
    if (pop == NULL) {
		perror(pool_name);
		exit(1);
	  } 
    PMEMoid root = pmemobj_root(pop, sizeof (ExtendibleHash));
    struct ExtendibleHash *rootp = (ExtendibleHash*)pmemobj_direct(root);
    if (rootp == NULL)
		{
			perror("Pointer error");
			exit(1);
		}
    cout << rootp->hash_name() << endl;
    cout << "Read return the pointer to Extendible Hashing" << std::endl;
    return rootp;
  }
  cout << "error" << std::endl;
  return NULL;
}




void ExtendibleHash::init_Extend(PMEMobjpool *_pop, ExtendibleHash *extend,size_t init_size){
  TX_BEGIN(_pop){
    pmemobj_tx_add_range_direct(extend,sizeof(class ExtendibleHash));
   //modify the parameters
    extend->pop = _pop;
    extend->global_depth = static_cast<size_t>(log2(init_size));
    extend->dir = new Directory(init_size);
    cout << "new dirctory ends" << endl;
    for (unsigned i = 0; i < dir->capacity; ++i) {
       dir->_[i] = new Block(global_depth);;
       dir->_[i]->pattern = i;
       //cout << "set Block point ends for " << i <<endl;
       for (int j = 0; j <  dir->_[i]->kNumSlot ; j++){
          dir->_[i]->_[j].key = INVALID;
       }
    }
    //cout << "directory set over" << endl;
  }
  TX_ONABORT { printf("Init txn fails\n"); }
  TX_END

  cout << "init ends" <<endl;
}



int Block::Insert(PMEMobjpool *pop,Key_t& key, Value_t value, size_t key_hash) {
#ifdef INPLACE
  if (sema == -1) return -2;
#ifdef LSB
  if ((key_hash & (size_t)pow(2, local_depth)-1) != pattern) return -2;
#else
  if ((key_hash >> (8*sizeof(key_hash)-local_depth)) != pattern) return -2;
#endif
  auto lock = sema;
  int ret = -1;
  while (!CAS(&sema, &lock, lock+1)) {
    lock = sema;
  }
  Key_t LOCK = INVALID;
  for (unsigned i = 0; i < kNumSlot; ++i) {
#ifdef LSB
    if ((h(&_[i].key,sizeof(Key_t)) & (size_t)pow(2, local_depth)-1) != pattern) {
#else
    if ((h(&_[i].key,sizeof(Key_t)) >> (8*sizeof(key_hash)-local_depth)) != pattern) {
#endif
      _[i].key = INVALID;
      // auto invalid = _[slot].key;
      // CAS(&_[slot].key, &invalid, INVALID);
      // do I need clflush? i don't think so. as long as it is shared through cache..
    }
    if (CAS(&_[i].key, &LOCK, SENTINEL)) {
      _[i].value = value;
      mfence();
      _[i].key = key;
      ret = i;
      break;
    } else {
      LOCK = INVALID;
    }
  }
  lock = sema;
  while (!CAS(&sema, &lock, lock-1)) {
    lock = sema;
  }
  return ret;
#else
  if (sema == -1) return -2;
#ifdef LSB
  if ((key_hash & (size_t)pow(2, local_depth)-1) != pattern) return -2;
#else
  if ((key_hash >> (8*sizeof(key_hash)-local_depth)) != pattern) return -2;
#endif
  auto lock = sema;
  int ret = -1;
  while (!CAS(&sema, &lock, lock+1)) {
    lock = sema;
  }
  Key_t LOCK = INVALID;
  for (unsigned i = 0; i < kNumSlot; ++i) {
    if (CAS(&_[i].key, &LOCK, SENTINEL)) {
      _[i].value = value;
      _[i].key = key;
      pmemobj_persist(pop,&_[i],sizeof(Pair));
      ret = i;
      break;
    } else {
      LOCK = INVALID;
    }
  }
  lock = sema;
  while (!CAS(&sema, &lock, lock-1)) {
    lock = sema;
  }
  return ret;
#endif
}

void Block::Insert4split(PMEMobjpool *pop, Key_t& key, Value_t value) {
  for (unsigned i = 0; i < kNumSlot; ++i) {
    if (_[i].key == INVALID) {
      _[i].key = key;
      _[i].value = value;
      return;
    }
  }
}

Block** Block::Split(PMEMobjpool *pop) {
  using namespace std;
  int64_t lock = 0;
  if (!CAS(&sema, &lock, -1)) return nullptr;
  // cout << this << " " << this_thread::get_id() << endl;

#ifdef INPLACE
  Block** split = new Block*[2];
  split[0] = this;
  split[1] = new Block(local_depth+1);

  for (unsigned i = 0; i < kNumSlot; ++i) {
    auto key_hash = h(&_[i].key, sizeof(Key_t));
#ifdef LSB
    if (key_hash & ((size_t) 1 << local_depth)) {
#else
    if (key_hash & ((size_t) 1 << ((sizeof(Key_t)*8 - local_depth - 1)))) {
#endif
      split[1]->Insert4split(_[i].key, _[i].value);
    }
  }

  clflush((char*)split[1], sizeof(Block));
  local_depth = local_depth + 1;
  clflush((char*)&local_depth, sizeof(size_t));

  return split;
#else
  Block** split;
  TX_BEGIN(pop){
  split = new Block*[2];
  split[0] = new Block(local_depth+1);
  split[1] = new Block(local_depth+1);

  for (unsigned i = 0; i < kNumSlot; ++i) {
    auto key_hash = h(&_[i].key, sizeof(Key_t));
#ifdef LSB
    if (key_hash & ((size_t) 1 << (local_depth))) {
#else
    if (key_hash & ((size_t) 1 << ((sizeof(Key_t)*8 - local_depth - 1)))) {
#endif
      split[1]->Insert4split(pop,_[i].key, _[i].value);
    } else {
      split[0]->Insert4split(pop,_[i].key, _[i].value);
    }
  }
  pmemobj_persist(pop,(char*)split[0], sizeof(Block));
  pmemobj_persist(pop,(char*)split[1], sizeof(Block));
  // cout << split[0]->numElem() << " " << split[1]->numElem() << endl;
  }TX_END
  //cout <<"split ends" <<endl;
  return split;
#endif
}


ExtendibleHash::ExtendibleHash(void){}

ExtendibleHash::ExtendibleHash(size_t initCap)
{
  Directory *dir = new Directory(initCap);
  global_depth = static_cast<size_t>(log2(initCap));
  for (unsigned i = 0; i < dir->capacity; ++i) {
    dir->_[i] = new Block(global_depth);
    dir->_[i]->pattern = i;
  }
}

ExtendibleHash::~ExtendibleHash(void)
{ }

void Directory::LSBUpdate(PMEMobjpool *pop, int local_depth, int global_depth, int dir_cap, int x, Block** s) {
  int depth_diff = global_depth - local_depth;
  if (depth_diff == 0) {
    if ((x % dir_cap) >= dir_cap/2) {
      _[x-dir_cap/2] = s[0];
      //clflush((char*)&_[x-dir_cap/2], sizeof(Block*));
      pmemobj_persist(pop,(char*)_[x-dir_cap/2], sizeof(Block*));
      _[x] = s[1];
      //clflush((char*)&_[x], sizeof(Block*));
      pmemobj_persist(pop,(char*)_[x], sizeof(Block*));
    } else {
      _[x] = s[0];
      //clflush((char*)&_[x], sizeof(Block*));
      pmemobj_persist(pop,(char*)&_[x], sizeof(Block*));
      _[x+dir_cap/2] = s[1];
      //clflush((char*)&_[x+dir_cap/2], sizeof(Block*));
      pmemobj_persist(pop,(char*)&_[x+dir_cap/2], sizeof(Block*));
    }
  } else {
    if ((x%dir_cap) >= dir_cap/2) {
      LSBUpdate(pop,local_depth+1, global_depth, dir_cap/2, x-dir_cap/2, s);
      LSBUpdate(pop,local_depth+1, global_depth, dir_cap/2, x, s);
    } else {
      LSBUpdate(pop,local_depth+1, global_depth, dir_cap/2, x, s);
      LSBUpdate(pop,local_depth+1, global_depth, dir_cap/2, x+dir_cap/2, s);
    }
  }
  return;
}

bool ExtendibleHash::Insert(Key_t& key, Value_t value) {
  using namespace std;
  // timer.Start();
  auto key_hash = h(&key, sizeof(key));
  // timer.Stop();
  // breakdown += timer.GetSeconds();
  bool resized = false;

RETRY:
#ifdef LSB
  // auto x = (key_hash & (pow(2, global_depth)-1));
  auto x = (key_hash % dir->capacity);
#else
  auto x = (key_hash >> (8*sizeof(key_hash)-global_depth));
#endif
  auto target = dir->_[x];
  // timer.Start();
  auto ret = target->Insert(pop,key, value, key_hash);
  // timer.Stop();
  // breakdown += timer.GetSeconds();

  if (ret == -1) {
    resized = true;
    Block** s = target->Split(pop);
    if (s == nullptr) {
      // another thread is doing split
      goto RETRY;
    }

    // timer.Start();
    // cout << x << " " << y << endl;
  // cout << x << " " << target->numElem() << endl;
#ifdef LSB
    s[0]->pattern = (key_hash % (size_t)pow(2, s[0]->local_depth-1));
    s[1]->pattern = s[0]->pattern + (1 << (s[0]->local_depth-1));
    //cout << s[0]->pattern << endl;
    //cout << s[1]->pattern << endl;
    // cout << bitset<16>(key_hash) << endl;
    // cout << bitset<16>(s[0]->pattern) << endl;
    // cout << bitset<16>(s[1]->pattern) << endl;
#else
    s[0]->pattern = (key_hash >> (8*sizeof(key_hash)-s[0]->local_depth+1)) << 1;
    s[1]->pattern = ((key_hash >> (8*sizeof(key_hash)-s[1]->local_depth+1)) << 1) + 1;
#endif

    // Directory management
    while (!dir->Acquire()) {
      // lockCount ++;
    }
    // dir->sema++;
    { // CRITICAL SECTION - directory update
    // timer.Start();
      // auto prev = x;
#ifdef LSB
      x = (key_hash % dir->capacity);
#else
      x = (key_hash >> (8*sizeof(key_hash)-global_depth));
#endif
#ifdef INPLACE
      if (dir->_[x]->local_depth-1 < global_depth) {  // normal split
#else
      if (dir->_[x]->local_depth < global_depth) {  // normal split
#endif
#ifdef LSB
        dir->LSBUpdate(pop,s[0]->local_depth, global_depth, dir->capacity, x, s);
        //cout <<"LSB Update ends" <<endl;
#else
        unsigned depth_diff = global_depth - s[0]->local_depth;
        if (depth_diff == 0) {
          if (x%2 == 0) {
            dir->_[x+1] = s[1];
#ifdef INPLACE
            clflush((char*) &dir->_[x+1], 8);
#else
            mfence();
            dir->_[x] = s[0];
            clflush((char*) &dir->_[x], 16);
#endif
          } else {
            dir->_[x] = s[1];
#ifdef INPLACE
            clflush((char*) &dir->_[x], 8);
#else
            mfence();
            dir->_[x-1] = s[0];
            clflush((char*) &dir->_[x-1], 16);
#endif
          }
        } else {
          int chunk_size = pow(2, global_depth - (s[0]->local_depth - 1));
          x = x - (x % chunk_size);
          for (unsigned i = 0; i < chunk_size/2; ++i) {
            dir->_[x+chunk_size/2+i] = s[1];
          }
          clflush((char*)&dir->_[x+chunk_size/2], sizeof(void*)*chunk_size/2);
#ifndef INPLACE
          for (unsigned i = 0; i < chunk_size/2; ++i) {
            dir->_[x+i] = s[0];
          }
          clflush((char*)&dir->_[x], sizeof(void*)*chunk_size/2);
#endif
        }
#endif
        // cout << x << " normal split " << endl;
      } else {  // directory doubling
      //cout << "directory doubling" << endl;
        resized = true;
        auto d = dir->_;
        auto _dir = new Block*[dir->capacity*2];
#ifdef LSB
        memcpy(_dir, d, sizeof(Block*)*dir->capacity);
        memcpy(_dir+dir->capacity, d, sizeof(Block*)*dir->capacity);
        _dir[x] = s[0];
        _dir[x+dir->capacity] = s[1];
#else
        for (unsigned i = 0; i < dir->capacity; ++i) {
          if (i == x) {
            _dir[2*i] = s[0];
            _dir[2*i+1] = s[1];
          } else {
            // if (d[i] == target) {
            //   cout << i << " " << x << " " << target << " " << prev << endl;
            // }
            _dir[2*i] = d[i];
            _dir[2*i+1] = d[i];
          }
        }
#endif
        // for (unsigned i = 0; i < dir->capacity * 2; ++i) {
        //   if (_dir[i] == target) {
        //     cout << "SOMETHING WRONG " << i << endl;
        //   }
        // }
        pmemobj_persist(pop,(char*)&dir->_[0], sizeof(Block*)*dir->capacity);
        dir->_ = _dir;
        pmemobj_persist(pop,(char*)&dir->_, sizeof(void*));
        dir->capacity *= 2;
        pmemobj_persist(pop,(char*)&dir->capacity, sizeof(size_t));
        global_depth += 1;
        pmemobj_persist(pop,(char*)&global_depth, sizeof(global_depth));
        // cout << global_depth << endl;
        //delete d;
        //cout <<"delete d should be implemented" <<endl;
        // TODO: requiered to do this atomically
        // cout << x << " directory doubling " << target << " " << dir->_[x]<< endl;
      }
    // timer.Stop();
    // breakdown += timer.GetSeconds();
#ifdef INPLACE
      s[0]->sema = 0;
#endif
    }  // End of critical section
    // dir->sema--;
    while (!dir->Release()) {
      // lockCount ++;
    }
    // timer.Stop();
    // breakdown += timer.GetSeconds();
    goto RETRY;
  } else if (ret == -2) {
    Insert(key, value);
  } else {
    //clflush((char*)&dir->_[x]->_[ret], sizeof(Pair));
    pmemobj_persist(pop,(char*)&dir->_[x]->_[ret], sizeof(Pair));
  }
  //cout <<"resizing ends" <<endl;
  return resized;
}

// This function does not allow resizing
bool ExtendibleHash::InsertOnly(Key_t& key, Value_t value) {
  auto key_hash = h(&key, sizeof(key));
#ifdef LSB
  auto x = (key_hash % dir->capacity);
#else
  auto x = (key_hash >> (8*sizeof(key_hash)-global_depth));
#endif

  auto ret = dir->_[x]->Insert(pop,key, value, key_hash);
  if (ret > -1) {
    //clflush((char*)&dir->_[x]->_[ret], sizeof(Pair));
    pmemobj_persist(pop,(char*)&dir->_[x]->_[ret], sizeof(Pair));
    return true;
  }

  return false;
}

// TODO
bool ExtendibleHash::Delete(Key_t& key) {
    auto key_hash = h(&key, sizeof(key));
#ifdef LSB
  auto x = (key_hash % dir->capacity);
#else
  auto x = (key_hash >> (8*sizeof(key_hash)-global_depth));
#endif

  auto dir_ = dir->_[x];

 for (unsigned i = 0; i < Block::kNumSlot; ++i) {
    if (dir_->_[i].key == key) {
#ifdef INPLACE
      sema = dir->_[x]->sema;
      while (!CAS(&dir->_[x]->sema, &sema, sema-1)) {
        sema = dir->_[x]->sema;
      }
#endif
      dir_->_[i].key = INVALID;
      pmemobj_persist(pop, &dir_->_[i].key, sizeof(key));
      return true;
    }
    lockCount++;
  }

#ifdef INPLACE
  sema = dir->_[x]->sema;
  while (!CAS(&dir->_[x]->sema, &sema, sema-1)) {
    sema = dir->_[x]->sema;
  }
#endif
  return false;
}

Value_t ExtendibleHash::Get(Key_t& key) {
  auto key_hash = h(&key, sizeof(key));
#ifdef LSB
  auto x = (key_hash % dir->capacity);
#else
  auto x = (key_hash >> (8*sizeof(key_hash)-global_depth));
#endif

  auto dir_ = dir->_[x];

#ifdef INPLACE
  auto sema = dir->_[x]->sema;
  while (!CAS(&dir->_[x]->sema, &sema, sema+1)) {
    sema = dir->_[x]->sema;
  }
#endif

  for (unsigned i = 0; i < Block::kNumSlot; ++i) {
    if (dir_->_[i].key == key) {
#ifdef INPLACE
      sema = dir->_[x]->sema;
      while (!CAS(&dir->_[x]->sema, &sema, sema-1)) {
        sema = dir->_[x]->sema;
      }
#endif
      return dir_->_[i].value;
    }
    lockCount++;
  }

#ifdef INPLACE
  sema = dir->_[x]->sema;
  while (!CAS(&dir->_[x]->sema, &sema, sema-1)) {
    sema = dir->_[x]->sema;
  }
#endif
  return NONE;
}


bool ExtendibleHash::Positive_Get(Key_t& key) {
  auto key_hash = h(&key, sizeof(key));
#ifdef LSB
  auto x = (key_hash % dir->capacity);
#else
  auto x = (key_hash >> (8*sizeof(key_hash)-global_depth));
#endif

  auto dir_ = dir->_[x];

#ifdef INPLACE
  auto sema = dir->_[x]->sema;
  while (!CAS(&dir->_[x]->sema, &sema, sema+1)) {
    sema = dir->_[x]->sema;
  }
#endif

  for (unsigned i = 0; i < Block::kNumSlot; ++i) {
    if (dir_->_[i].key == key || dir_->_[i].key != key  ) {
#ifdef INPLACE
      sema = dir->_[x]->sema;
      while (!CAS(&dir->_[x]->sema, &sema, sema-1)) {
        sema = dir->_[x]->sema;
      }
#endif
    Value_t get_value = dir_->_[i].value;
    return true;
    }
    lockCount++;
  }

#ifdef INPLACE
  sema = dir->_[x]->sema;
  while (!CAS(&dir->_[x]->sema, &sema, sema-1)) {
    sema = dir->_[x]->sema;
  }
#endif
  return false;
}

bool ExtendibleHash::Negative_Get(Key_t& key) {
  auto key_hash = h(&key, sizeof(key));
#ifdef LSB
  auto x = (key_hash % dir->capacity);
#else
  auto x = (key_hash >> (8*sizeof(key_hash)-global_depth));
#endif

  auto dir_ = dir->_[x];

#ifdef INPLACE
  auto sema = dir->_[x]->sema;
  while (!CAS(&dir->_[x]->sema, &sema, sema+1)) {
    sema = dir->_[x]->sema;
  }
#endif

  for (unsigned i = 0; i < Block::kNumSlot; ++i) {
    if (dir_->_[i].key == key && dir_->_[i].key != key ) {
#ifdef INPLACE
      sema = dir->_[x]->sema;
      while (!CAS(&dir->_[x]->sema, &sema, sema-1)) {
        sema = dir->_[x]->sema;
      }
#endif
      return true;
    }
    lockCount++;
  }

#ifdef INPLACE
  sema = dir->_[x]->sema;
  while (!CAS(&dir->_[x]->sema, &sema, sema-1)) {
    sema = dir->_[x]->sema;
  }
#endif
  return false;
}



// Debugging function
Value_t ExtendibleHash::FindAnyway(Key_t& key) {
  using namespace std;
  for (size_t i = 0; i < dir->capacity; ++i) {
     for (size_t j = 0; j < Block::kNumSlot; ++j) {
       if (dir->_[i]->_[j].key == key) {
         auto key_hash = h(&key, sizeof(key));
         auto x = (key_hash >> (8*sizeof(key_hash)-global_depth));
         return dir->_[i]->_[j].value;
       }
     }
  }
  return NONE;
}

// Not accurate
double ExtendibleHash::Utilization(void) {
  size_t sum = 0;
  std::unordered_map<Block*, bool> set;
  for (size_t i = 0; i < dir->capacity; ++i) {
    set[dir->_[i]] = true;
  }
  for (auto& elem: set) {
    for (unsigned i = 0; i < Block::kNumSlot; ++i) {
      if (elem.first->_[i].key != INVALID) sum++;
    }
  }
  return ((double)sum)/((double)set.size()*Block::kNumSlot)*100.0;
}

void Directory::SanityCheck(void* addr) {
  using namespace std;
  for (unsigned i = 0; i < capacity; ++i) {
    if (_[i] == addr) {
      cout << i << " " << _[i]->sema << endl;
      exit(1);
    }
  }
}

size_t ExtendibleHash::Capacity(void) {
  std::unordered_map<Block*, bool> set;
  for (size_t i = 0; i < dir->capacity; ++i) {
    set[dir->_[i]] = true;
  }
  return set.size() * Block::kNumSlot;
}

size_t Block::numElem(void) {
  size_t sum = 0;
  for (unsigned i = 0; i < kNumSlot; ++i) {
    if (_[i].key != INVALID) {
      sum++;
    }
  }
  return sum;
}

//header file ends


bool ExtendibleHash::Recovery(){
  TX_BEGIN(pop){
    pmemobj_tx_add_range_direct(this,sizeof(class ExtendibleHash));
  for(int i = 0; i < dir->capacity;i++){
    int local_depth = 0;
    for(int j = 0; j < dir->_[i]->kNumSlot;j++){
      if(dir->_[i]->_[j].key!=INVALID){
        local_depth++;
      }
    }
    dir->_[i]->local_depth = local_depth;
  }
  }TX_END
  return true;
}

int main(int argc, char *argv[]){
  // Step 1: create (if not exist) and open the pool
	  cout << "Testing Begin" <<std::endl;
    bool file_exist = false;
    if (FileExists(pool_name)) file_exist = true;

  if (!file_exist){
		PMEMobjpool *pop;
    	pop = pmemobj_create(pool_name, "Extendible Hash", 1024 * 1024 *128 * 32UL, 0666);
    	if (pop == NULL) {
			perror(pool_name);
			std::exit(1);
		}
		    //size_t segment_number = (size_t)argv[1];
    size_t init_size = 2;
    PMEMoid root = pmemobj_root(pop, sizeof (ExtendibleHash));
		struct ExtendibleHash *rootp = (ExtendibleHash*)pmemobj_direct(root);
    new (rootp) ExtendibleHash();
		rootp->init_Extend(pop,rootp,init_size);
    cout << "Current capacity of Extendible hashing is " << rootp->dir->capacity << endl;
		if (rootp == NULL)
		{
			perror("Pointer error");
			std::exit(1);
		}
    cout << "insertion start" <<endl;
		int already_exists = 0;
		for (uint64_t i = 0; i < READ_WRITE_NUM; ++i) {
    	// Enroll into the epoch, if using one thread, epoch mechanism is actually
			for (uint64_t j = 0; j < READ_WRITE_NUM; ++j) {
			uint64_t Key = i * 1024 + j;
			auto ret = rootp->Insert(Key, "1");
			//cout << "Insert Ends for key " << i * 1024 + j<< std::endl;
			if (ret == -1) already_exists++;
		}
    }

	double single_time;
	struct timespec start, finish;
 	clock_gettime(CLOCK_MONOTONIC, &start);	
  rootp->Recovery();
  clock_gettime(CLOCK_MONOTONIC, &finish);
  single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;

  printf("Recovery time: %f second \n", single_time);

	pmemobj_close(pop);	
	}
	cout << "testing ends" <<std::endl;
	// Close PMEM object pool	
	return 0;
}