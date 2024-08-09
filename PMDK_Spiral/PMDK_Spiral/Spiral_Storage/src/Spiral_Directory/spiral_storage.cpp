
#include "spiral_storage.h"

// pool path and name
static const char *pool_name = "pmem_hash.data";
//static const char *pool_name = "../../../../mnt/pmem0/wpeng/pmem_spiral.data";
// pool size
static const size_t pool_size = 1024ul * 1024 * 10ul;

#define READ_WRITE_NUM 100

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



// void SpiralStorage::resize(PMEMobjpool *_pop){
//   //cout << "Resizing start" << endl;

//   uint64_t new_size = size + STEP;
//   uint64_t new_capacity = capacity + STEP * ASSOC_NUM;
//   //auto ret = pmemobj_realloc(pop, &this->_buckets, sizeof(Node) * new_size, TOID_TYPE_NUM(char));
//   //buckets = (Node *)cache_align(pmemobj_direct(this->_buckets));
//   auto d = buckets;
//   auto _dir = new Node * [new_size];
//   memcpy(_dir, d, sizeof(Node*)*size);
//   PMEMoid _new_bucket;
//   auto ret = pmemobj_zalloc(pop,&_new_bucket,sizeof(Node), TOID_TYPE_NUM(char));
//   Node * new_bucket = (Node *)cache_align(pmemobj_direct(_new_bucket));

//   _dir[size] = new_bucket;

//   if(ret){
// 	cout << "Ret is" << ret << endl;
// 	exit(0);
//   }
//   TX_BEGIN(pop) {
// 	pmemobj_tx_add_range_direct(this, sizeof(class SpiralStorage));
// 	pmemobj_tx_add_range_direct(_dir[size], sizeof(Node));
//     pmemobj_tx_add_range_direct(_dir[expand_location], sizeof(Node));
// 	uint64_t first = round(pow(b,c));
// 	expand_location = look_up_table[first];
// 	_dir[size] = _dir[expand_location];
// 	_dir[expand_location] = new_bucket; 
// 	buckets = _dir;
//     c = log(first+1)/log(b);
//     capacity = new_capacity;
//     size = new_size;
//   }
//   TX_ONABORT { printf("resizing txn 2 fails\n"); }
//   TX_END
  
//   //cout << "New c is set to be " << c <<endl;
//   //cout << "Current size is " << size <<endl;
//   //cout << "Done! Resizing Finsihed for location " << expand_location << endl;
// }

// void SpiralStorage::resize(PMEMobjpool *_pop){
//   //cout << "Resizing start" << endl;

//   uint64_t new_size = size + STEP;
//   uint64_t new_capacity = capacity + STEP * ASSOC_NUM;
//   //auto ret = pmemobj_realloc(pop, &this->_buckets, sizeof(Node) * new_size, TOID_TYPE_NUM(char));
//   //buckets = (Node *)cache_align(pmemobj_direct(this->_buckets));
//   auto d = buckets;
//   auto _dir = new Node * [new_size];
//   memcpy(_dir, d, sizeof(Node*)*size);
//   PMEMoid _new_bucket;
//   auto ret = pmemobj_zalloc(pop,&_new_bucket,sizeof(Node), TOID_TYPE_NUM(char));
//   Node * new_bucket = (Node *)cache_align(pmemobj_direct(_new_bucket));

//   _dir[size] = new_bucket;

//   if(ret){
// 	cout << "Ret is" << ret << endl;
// 	exit(0);
//   }
// 	uint64_t first = round(pow(b,c));
// 	expand_location = look_up_table[first];
// 	_dir[size] = _dir[expand_location];
// 	_dir[expand_location] = new_bucket; 
// 	buckets = _dir;

//     c = log(first+1)/log(b);
// 	pmemobj_persist(pop,&c,sizeof(c));
//     capacity = new_capacity;
// 	pmemobj_persist(pop,&capacity,sizeof(capacity));
//     size = new_size;
// 	pmemobj_persist(pop,&size,sizeof(size));
//   //cout << "New c is set to be " << c <<endl;
//   //cout << "Current size is " << size <<endl;
//   //cout << "Done! Resizing Finsihed for location " << expand_location << endl;
// }



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
		for (uint64_t i = 0; i < READ_WRITE_NUM; ++i) {
    	// Enroll into the epoch, if using one thread, epoch mechanism is actually
			for (uint64_t j = 0; j < READ_WRITE_NUM; ++j) {
			uint64_t Key = i * 1024 + j;
      	//cout << "Key address is " << &Key <<endl;
			auto ret = rootp->insert((char*)&Key, sizeof(&Key),value,sizeof(value),0,0);
			//cout << "Insert Ends for key " << i * 1024 + j << endl;
			if (ret == false) already_exists++;
		  }
    }
	rootp->print_all();
	cout << "already exists is " << already_exists << endl;
	cout << "the item number of Spiral Storage is " << rootp->item_num << endl;
	cout << "the size of Spiral Storage is " << rootp->size << endl;
	cout << "the capacity of Spiral Storage is " << rootp->capacity << endl;
	cout << "the threshold of spiral storage is " << rootp->capacity * rootp->threshold << endl;

	double single_time;
	struct timespec start, finish;
 	clock_gettime(CLOCK_MONOTONIC, &start);	

    int positive_search = 0;
		for (uint64_t i = 0; i < READ_WRITE_NUM; ++i) {
    	// Enroll into the epoch, if using one thread, epoch mechanism is actually
			for (uint64_t j = 0; j < READ_WRITE_NUM; ++j) {
			uint64_t Key = i * 1024 + j;
			auto ret = rootp->Positive_Get(Key);
			//cout << "Search Ends for key " << i * 1024 + j << endl;
			if (ret == 1){
				positive_search++;
			}
		  }
    }
	clock_gettime(CLOCK_MONOTONIC, &finish);
	cout <<"positive search number is " << positive_search << endl;

	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;

	printf("Run phase throughput: %f positive search per second \n", READ_WRITE_NUM*READ_WRITE_NUM/single_time);

	int negative_search = 0;
	clock_gettime(CLOCK_MONOTONIC, &start);	
		for (uint64_t i = 0; i < READ_WRITE_NUM; ++i) {
    	// Enroll into the epoch, if using one thread, epoch mechanism is actually
			for (uint64_t j = 0; j < READ_WRITE_NUM; ++j) {
			uint64_t Key = i * 1024 + j;
			auto ret = rootp->Negative_Get(Key);
			//cout << "Search Ends for key " << i * 1024 + j << endl;
			if (ret == 0){
				negative_search++;
			}
		  }
    }
	clock_gettime(CLOCK_MONOTONIC, &finish);
	cout <<"negative search number is " << negative_search << endl;
	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
	printf("Run phase throughput: %f negative search per second \n", READ_WRITE_NUM*READ_WRITE_NUM/single_time);
	pmemobj_close(pop);	
	}
	cout << "testing ends" <<endl;
	// Close PMEM object pool
		
	return 0;
}