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



int main(int argc, char *argv[]){
  // Step 1: create (if not exist) and open the pool
	cout << "Testing Begin" <<endl;
    bool file_exist = false;
    if (FileExists(pool_name)) file_exist = true;

	if (!file_exist){
		PMEMobjpool *pop;
    	pop = pmemobj_create(pool_name, "Spiral_Storage", 1024 * 1024 * 32UL, 0666);
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
  uint8_t key[8];
  uint8_t value[8];
  FILE *ycsb, *ycsb_read;
	char *buf = NULL;
	size_t len = 0;
  int inserted = 0;
  double single_time;
  struct timespec start, finish;
  if((ycsb = fopen("../../YCSB_workload/rw-50-50-load.txt","r")) == NULL)
    {
        perror("fail to read");
    }
  cout <<"Load phase begins" <<endl;
	while(getline(&buf,&len,ycsb) != -1){
		if(strncmp(buf, "INSERT", 6) == 0){
			memcpy(key, buf+7, 7);
			if (rootp->insert((const char *)&key,8,(const char *)&key,8,0,0))                      
			{
				inserted ++;
			}
		}
	}
  fclose(ycsb);
  printf("Load phase finishes: %d items are inserted \n", rootp->item_num); 
  int updated;
  int read;



  if((ycsb_read = fopen("../../YCSB_workload/rw-50-50-run.txt","r")) == NULL)
    {
        perror("fail to read");
    }
	clock_gettime(CLOCK_MONOTONIC, &start);	
  while(getline(&buf,&len,ycsb) != -1){
		if(strncmp(buf, "UPDATE", 4) == 0){
			memcpy(key, buf+7, 7);
			if (rootp->update((const char *)&key,8,(const char *)&key,8))                      
			{
        	cout << "update success" << endl;
			updated++;
			}
    }
    if(strncmp(buf, "READ", 4) == 0){
			memcpy(key, buf+7, 7);
			if (rootp->find((const char *)&key,8,(char *)&key,0))                      
			{
        	cout << "read success" << endl;
				read++;
			}
		}
	}
	clock_gettime(CLOCK_MONOTONIC, &finish);
	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
  	cout << "time usage is " <<single_time<< endl;
	printf("Run phase throughput: %f M operations per second \n", single_time/1000);	
	pmemobj_close(pop);	
	}
	cout << "testing ends" <<endl;
	// Close PMEM object pool
	return 0;
}