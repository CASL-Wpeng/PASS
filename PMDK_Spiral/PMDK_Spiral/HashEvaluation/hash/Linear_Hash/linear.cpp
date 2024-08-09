#include "linear.h"

static const char *pool_name = "pmem_hash.data";


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
			for (uint64_t j = 0; j < 10; ++j) {
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