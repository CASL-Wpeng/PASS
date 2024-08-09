#include "spiral_storage.h"

// pool path and name
static const char *pool_name = "pmem_hash.data";
// pool size
static const size_t pool_size = 1024ul * 1024 * 10ul;

const Value_t DEFAULT = reinterpret_cast<Value_t>(1);

#define READ_WRITE_NUM 160

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

static void * ycsb_thread_run(void * arg){
    sub_thread* subthread = (sub_thread *)arg;
    uint8_t key[KEY_LEN];
    uint8_t value[VALUE_LEN]; 
    int i = 0;
    cout << READ_WRITE_NUM/subthread->spiral->thread_num << endl;
    printf("Thread %d is opened\n", subthread->id);
    for(; i < READ_WRITE_NUM/subthread->spiral->thread_num; i++){
        if(subthread->run_queue[i].operation == 1){
            if (!subthread->spiral->Insert(subthread->run_queue[i].key,(Value_t)subthread->run_queue[i].key)){   
                subthread->inserted ++;
            }
        }else{
            if(subthread->spiral->Get(subthread->run_queue[i].key)!= NONE)
                // Get value
                cout << "Get value" <<endl;
        }
        //cout << "testing iteration " << i << endl;
    }
    pthread_exit(NULL);
}



int main(int argc, char *argv[]){
  // Step 1: create (if not exist) and open the pool
	cout << "Testing Begin" <<endl;
  bool file_exist = false;
  if (FileExists(pool_name)) file_exist = true;

  // prepare YCSB workload

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
  uint8_t key[KEY_LEN];
  uint8_t value[VALUE_LEN];
  FILE *ycsb, *ycsb_read;
	char *buf = NULL;
	size_t len = 0;
  int inserted = 0;
  double single_time;
  struct timespec start, finish;
  if((ycsb = fopen("../../YCSB_workload/rw-50-50-load.txt","r")) == NULL)
    {
        perror("fail to read");
        exit(1);
    }
  cout <<"Load phase begins" <<endl;
	while(getline(&buf,&len,ycsb) != -1){
		if(strncmp(buf, "INSERT", 6) == 0){
			memcpy(key, buf+7, KEY_LEN-1);
			if (rootp->insert((const char *)&key,KEY_LEN,(const char *)&key,KEY_LEN,0,0))                      
			{
				inserted ++;
			}
      if (inserted == READ_WRITE_NUM){
        break;
      }
		}
	}
	fclose(ycsb);
  printf("Load phase finishes: %d items are inserted \n", inserted);


  if((ycsb_read = fopen("../../YCSB_workload/rw-50-50-run.txt","r")) == NULL)
    {
        perror("fail to read");
    }
    int thread_num = 2;
    rootp->thread_num = 2;
    pmemobj_persist(pop,&rootp->thread_num,sizeof(rootp->thread_num));
    
    thread_queue* run_queue[thread_num];
    int move[thread_num];
    for(int t = 0; t < thread_num; t ++){
        run_queue[t] = (thread_queue* )calloc(READ_WRITE_NUM/thread_num, sizeof(thread_queue));
        move[t] = 0;
    }
	int operation_num = 0;		
	while(getline(&buf,&len,ycsb_read) != -1){
		if(strncmp(buf, "UPDATE", 6) == 0){
      memcpy(key, buf+7, KEY_LEN-1);
      uint64_t i, k = 0;
      for (i = 0; i < KEY_LEN; i++)
        k = 10 * k + key[i];
			run_queue[operation_num%thread_num][move[operation_num%thread_num]].key = k;
			run_queue[operation_num%thread_num][move[operation_num%thread_num]].operation = 1;
			move[operation_num%thread_num] ++;
		}
		else if(strncmp(buf, "READ", 4) == 0){
      memcpy(key, buf+7, KEY_LEN-1);
      uint64_t i, k = 0;
      for (i = 0; i < KEY_LEN; i++)
        k = 10 * k + key[i];
			run_queue[operation_num%thread_num][move[operation_num%thread_num]].key = k;
			run_queue[operation_num%thread_num][move[operation_num%thread_num]].operation = 0;
			move[operation_num%thread_num] ++;
		}
		operation_num ++;
    if (operation_num == READ_WRITE_NUM){
        break;
    }
	}
	fclose(ycsb_read);
	sub_thread* THREADS = (sub_thread*)malloc(sizeof(sub_thread)*thread_num);
    inserted = 0;

	cout << "Run phase begins: SEARCH/INSERTION ratio is 50/% 50/%" << endl;
    clock_gettime(CLOCK_MONOTONIC, &start);	
    cout << "testing1" << endl;
    for(int t = 0; t < thread_num; t++){
        THREADS[t].id = t;
        THREADS[t].spiral = rootp;
        THREADS[t].inserted = 0;
        THREADS[t].run_queue = run_queue[t];
        pthread_create(&THREADS[t].thread, NULL, &ycsb_thread_run, &THREADS[t]);
    }
    for(int t = 0; t < thread_num; t++){
        pthread_join(THREADS[t].thread, NULL);
    }

	clock_gettime(CLOCK_MONOTONIC, &finish);
	single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;

    for(int t = 0; t < thread_num; ++t){
        inserted +=  THREADS[t].inserted;
    }
    printf("Run phase finishes: %d/%d items are inserted/searched\n", operation_num - inserted, inserted);
    printf("Run phase throughput: %f operations per second \n", READ_WRITE_NUM/single_time);	
    
	pmemobj_close(pop);	
	}
	cout << "testing ends " <<endl;
	// Close PMEM object pool
		
	return 0;
}