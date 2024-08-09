
#include "spiral_storage.h"

// pool path and name
static const char *pool_name = "pmem_hash.data";
// pool size
static const size_t pool_size = 1024ul * 1024 * 10ul;

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



void SpiralStorage::resize(PMEMobjpool *_pop){
  //cout << "Resizing start" << endl;

  uint64_t new_size = size + STEP;
  uint64_t new_capacity = capacity + STEP * ASSOC_NUM;
  auto ret = pmemobj_realloc(pop, &this->_buckets, sizeof(Node) * new_size, TOID_TYPE_NUM(char));
  buckets = (Node *)cache_align(pmemobj_direct(this->_buckets));

	//cout << "test for dividing by zero 3" << endl;

	if (ret) {
		  cout << "Ret = " << ret << endl;
		  LOG_FATAL("Allocation Error in New Bucket");
	  }
  
  TX_BEGIN(pop) {
	pmemobj_tx_add_range_direct(this, sizeof(class SpiralStorage));
	pmemobj_tx_add_range_direct(buckets,sizeof(Node) * new_size);
    resizing = true;
	//cout << "test for dividing by zero 2" << endl;

    // Need to be filled to move some k-v pair to the new allocate bucket

    uint64_t first = round(pow(b,c));
    expand_location = look_up_table[first];
    // if(size >70 && size < 128){
	// 	cout << "test for resize location " << size << endl;
	// 	cout << "the expand location of first is " << expand_location << endl;
	// 	cout << "current b is " << b << endl;
	// 	cout << "current c is " << c << endl; 
	// 	//cout << buckets[expand_location].token[0] << endl;
	// }
    for(int i = 0;i < ASSOC_NUM; i++){
      if (buckets[expand_location].token[i] != 0){
        buckets[size].slot[i].key = buckets[expand_location].slot[i].key;
        buckets[size].slot[i].value = buckets[expand_location].slot[i].value;
        buckets[size].token[i] = 1;
        buckets[expand_location].token[i] = 0;
      }
    }
    c = log(first+1)/log(b);
    capacity = new_capacity;
    size = new_size;
    resizing = false;
  }
  TX_ONABORT { printf("resizing txn 2 fails\n"); }
  TX_END
  
  //cout << "New c is set to be " << c <<endl;
  cout << "Current size is " << size <<endl;
  //cout << "Done! Resizing Finsihed for location " << expand_location << endl;
}



int main(int argc, char *argv[]){
  // Step 1: create (if not exist) and open the pool
	cout << "Testing Begin" <<endl;
	Node *testing = new Node();
	cout << "the size of Node is " << sizeof(Node) << endl;
	cout << "the size of Node token is " << sizeof(testing->token) << endl;
	cout << "the size of Node slot is " << sizeof(testing->slot) << endl;
	//cout << "the size of Node dummy is " << sizeof(testing->dummy) << endl;
    bool file_exist = false;
    if (FileExists(pool_name)) file_exist = true;

	if (!file_exist){
		PMEMobjpool *pop;
    	pop = pmemobj_create(pool_name, "Spiral_Storage", 1024 * 1024 * 16 * 32UL, 0666);
    	if (pop == NULL) {
			perror(pool_name);
			exit(1);
		}
		    //size_t segment_number = (size_t)argv[1];
    size_t init_size = 1;
    cout << init_size << endl;
    PMEMoid root = pmemobj_root(pop, sizeof (SpiralStorage));
		struct SpiralStorage *rootp = (SpiralStorage*)pmemobj_direct(root);
    new (rootp) SpiralStorage();
		rootp->init_Spiral(pop,rootp,init_size);
		if (rootp== NULL)
		{
			perror("Pointer error");
			exit(1);
		}
		//rootp->Show_Look_Up_Table();
    //cout << "Threshold is " <<rootp->threshold << endl;
		int already_exists = 0;
    	char value[] = "1";
		for (uint64_t i = 0; i < 1; ++i) {
    	// Enroll into the epoch, if using one thread, epoch mechanism is actually
			for (uint64_t j = 0; j < 10; ++j) {
			uint64_t Key = i * 1024 + j;
      	//cout << "Key address is " << &Key <<endl;
			auto ret = rootp->insert((char*)&Key, sizeof(&Key),value,sizeof(value),0,0);
			cout << "Insert Ends for key " << i * 1024 + j << endl;
			if (ret == false) already_exists++;
		  }
    }
	//rootp->print_all();
	cout << "already exists is " << already_exists << endl;
	cout << "the item number of Spiral Storage is " << rootp->item_num << endl;
	cout << "the size of Spiral Storage is " << rootp->size << endl;
	cout << "the capacity of Spiral Storage is " << rootp->capacity << endl;
	cout << "the threshold of spiral storage is " << rootp->capacity * rootp->threshold << endl;

	//cout << rootp->buckets[128].slot[0].key << endl;
    // int positive_search = 0;
	// 	for (uint64_t i = 0; i < 1; ++i) {
    // 	// Enroll into the epoch, if using one thread, epoch mechanism is actually
	// 		for (uint64_t j = 0; j < 10; ++j) {
	// 		uint64_t Key = i * 1024 + j;
	// 		auto ret = rootp->Get(Key);
	// 		cout << "Search Ends for key " << i * 1024 + j << endl;
	// 		if (ret == "1") positive_search++;
	// 	  }
    // }
    // int positive_update = 0;
    // for (uint64_t i = 0; i < 1; ++i) {
    // 	// Enroll into the epoch, if using one thread, epoch mechanism is actually
	// 		for (uint64_t j = 0; j < 10; ++j) {
	// 		uint64_t Key = i * 1024 + j;
	// 		auto ret = rootp->Update(Key,"2");
	// 		cout << "Update Ends for key " << i * 1024 + j << endl;
	// 		if (ret == 1) positive_update++;
	// 	  }
    // }
    // int deletion = 0;
    // for (uint64_t i = 0; i < 1; ++i) {
    // 	// Enroll into the epoch, if using one thread, epoch mechanism is actually
	// 		for (uint64_t j = 0; j < 10; ++j) {
	// 		uint64_t Key = i * 1024 + j;
	// 		auto ret = rootp->Delete(Key);
	// 		cout << "Deletion Ends for key " << i * 1024 + j << endl;
	// 		if (ret == 1) deletion++;
	// 	  }
    // }    
	pmemobj_close(pop);	
	}
	cout << "testing ends" <<endl;
	// Close PMEM object pool
		
	return 0;
}