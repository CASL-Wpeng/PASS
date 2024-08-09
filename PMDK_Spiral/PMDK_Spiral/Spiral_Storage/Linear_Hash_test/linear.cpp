#include "linear.h"

static const char *pool_name = "pmem_hash.data";

//static const char *pool_name = "../../../../mnt/pmem0/wpeng/pmem_hash.data";

static bool FileExists(const char *pool_path) {
  struct stat buffer;
  if (stat(pool_path, &buffer) == 0){
	remove(pool_name);
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
    	pop = pmemobj_create(pool_name, "LinearHashing", 1024 * 1024 * 32UL, 0666);
    	if (pop == NULL) {
			perror(pool_name);
			std::exit(1);
		}
		//size_t segment_number = (size_t)argv[1];
    size_t init_size = sz >= 2 ? sz : 2;
    PMEMoid root = pmemobj_root(pop, sizeof (LinearHash));
		struct LinearHash *rootp = (LinearHash*)pmemobj_direct(root);
    new (rootp) LinearHash();
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


void LinearHash::resize(size_t _capacity) {
  cout << "resize start for new capacity " << _capacity << endl;
  //cout << "the current hash factor is " << hash_factor<<endl;

  for (int i = 0; i < nlocks; ++i) {
    pmemobj_rwlock_wrlock(pop, &mutex[i]);
  }
  int prev_nlocks = nlocks;
  nlocks = _capacity/locksize+1;
  pmemobj_persist(pop, &nlocks, sizeof(nlocks));

  _old_mutex = _mutex;
  pmemobj_persist(pop, &_old_mutex, sizeof(_old_mutex));
  

  //print_all();

  TX_BEGIN(pop) {
    pmemobj_tx_add_range_direct(this, sizeof(class LinearHash));
    PMEMoid _new_bucket = pmemobj_tx_zalloc(sizeof(Pair), TOID_TYPE_NUM(char));
    Pair * new_bucket = (Pair *)cache_align(pmemobj_direct(_new_bucket));
	auto d = dict;
	auto _dir = new Pair * [_capacity];
	memcpy(_dir, d, sizeof(Pair*)*capacity);
	pmemobj_persist(pop,&_dir,TOID_TYPE_NUM(char));
	pmemobj_tx_add_range_direct(new_bucket,sizeof(Pair));

	new_bucket->key =INVALID;
	new_bucket->value = NONE;
	_dir[capacity] = new_bucket;

	if(_capacity > hash_factor){
		hash_factor = hash_factor * 2;
		resize_loc = 0;
	}
	uint64_t resize_time = log2(hash_factor);
	uint64_t resize_item = 0;

	cout << "testing" << endl;
	for(int i = 0; i < resize_time ; i++){
		pmemobj_tx_add_range_direct(_dir[resize_loc + i*2],sizeof(Pair));
		if(_dir[resize_loc + i*2]->key != INVALID){
			//cout << "The hash key for key " << dir[resize_loc + i*2].key << " is " << h(&dir[resize_loc + i*2].key, sizeof(Key_t)) << endl;
			auto loc = h(&_dir[resize_loc + i*2]->key, sizeof(Key_t)) % hash_factor;
			//cout << "loc is " << loc << endl;
			if(loc > capacity) loc = loc / 2 ;
			if(loc != resize_loc + i*2){
			if (_dir[loc]->key == INVALID || _dir[loc]->value == NONE ){
				pmemobj_tx_add_range_direct(_dir[loc],sizeof(Pair));
				_dir[loc]->key = _dir[resize_loc + i*2]->key;
				_dir[loc]->value = _dir[resize_loc + i*2]->value;
				_dir[resize_loc + i*2]->key = INVALID;
				//cout << "item " << resize_loc + i*2 << " resize to " << loc <<endl;
			}
			else{
				_dir[resize_loc + i*2]->key = INVALID;
				_dir[resize_loc + i*2]->value = NONE;
				resize_item++;
				//cout << "item " << resize_loc + i*2 << " resize to " << loc <<" but failed because of existing record" << endl;
			}
			}
		} 
	}
	capacity = _capacity;
	old_cap = 0;
	resize_loc++;
	size = size - resize_item;
	dict = _dir;
  }TX_END

  //print_all();

  if(nlocks > prev_nlocks){
  auto ret = pmemobj_zalloc(pop,&_mutex,nlocks * sizeof(PMEMrwlock), TOID_TYPE_NUM(char));

  mutex = (PMEMrwlock *)(pmemobj_direct(_mutex));
  pmemobj_persist(pop,&mutex,sizeof(mutex));
  cout << "test for expanding the lock" << endl;
  TX_BEGIN(pop){
    pmemobj_tx_add_range_direct(this, sizeof(class LinearHash));
    pmemobj_tx_free(_old_mutex);
  }
  TX_ONABORT {printf("resizing txn 2 fails\n"); }
  TX_END
  }
}

//header ends, now main begins


int main(int argc, char *argv[]){
  // Step 1: create (if not exist) and open the pool
	cout << "Testing Begin" <<endl;
    bool file_exist = false;
    if (FileExists(pool_name)) file_exist = true;

  if (!file_exist){
		PMEMobjpool *pop;
    	pop = pmemobj_create(pool_name, "LinearHashing", 1024 * 1024 * 16 *  32UL, 0666);
    	if (pop == NULL) {
			perror(pool_name);
			std::exit(1);
		}
		    //size_t segment_number = (size_t)argv[1];
    size_t init_size = 2;
    PMEMoid root = pmemobj_root(pop, sizeof (LinearHash));
		struct LinearHash *rootp = (LinearHash*)pmemobj_direct(root);
    new (rootp) LinearHash();
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
			for (uint64_t j = 0; j < 100; ++j) {
			uint64_t Key = i * 1024 + j;
			auto ret = rootp->Insert(Key, "1");
			std::cout << "Insert Ends for key " << i * 1024 + j<< std::endl;
			if (ret == -1) already_exists++;
		  }
    }
	cout << "current size is " << rootp->size << endl;
	//rootp->print_all();
	pmemobj_close(pop);	
	}
	std::cout << "testing ends" <<std::endl;
	// Close PMEM object pool
	return 0;
}