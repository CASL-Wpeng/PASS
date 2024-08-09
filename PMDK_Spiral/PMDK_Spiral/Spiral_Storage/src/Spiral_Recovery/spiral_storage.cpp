
#include "spiral_storage.h"

// pool path and name
static const char *pool_name = "pmem_hash.data";
//static const char *pool_name = "../../../../../mnt/pmem0/wpeng/pmem_spiral.data";
// pool size
static const size_t pool_size = 1024ul * 1024 * 10ul;

#define READ_WRITE_NUM 500

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
	//rootp->print_all();
	cout << "already exists is " << already_exists << endl;
	cout << "the item number of Spiral Storage is " << rootp->item_num << endl;
	cout << "the size of Spiral Storage is " << rootp->size << endl;
	cout << "the capacity of Spiral Storage is " << rootp->capacity << endl;
	cout << "the threshold of spiral storage is " << rootp->capacity * rootp->threshold << endl;

	double single_time;
	struct timespec start, finish;
 	clock_gettime(CLOCK_MONOTONIC, &start);	
	rootp->Recovery2();
	clock_gettime(CLOCK_MONOTONIC, &finish);
	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;

	printf("Recovery time: %f second \n", single_time);

 	clock_gettime(CLOCK_MONOTONIC, &start);	

	auto ret = pmemobj_realloc(pop,&rootp->_old_dir,sizeof(Node*)* rootp->size, TOID_TYPE_NUM(char));
  	if(ret){
	cout << "Ret is" << ret << endl;
	exit(0);
  	}
	TX_BEGIN(pop){
    pmemobj_tx_add_range_direct(rootp,sizeof(class SpiralStorage));
	pmemobj_free(&rootp->_old_dir);
	}TX_END
	clock_gettime(CLOCK_MONOTONIC, &finish);
	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;

	printf("Recovery time: %f second \n", single_time);


	pmemobj_close(pop);	
	}
	cout << "testing ends" <<endl;
	// Close PMEM object pool
		
	return 0;
}