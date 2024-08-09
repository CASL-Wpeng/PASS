#include "linear.h"
#include <sys/stat.h>

using namespace std;

#define READ_WRITE_NUM 20

static const char *pool_name = "pmem_linear_hash.data";
//static const char *pool_name = "../../../../../mnt/pmem0/wpeng/pmem_linear_hash.data";

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
    cout << READ_WRITE_NUM/subthread->linear->thread_num << endl;
    printf("Thread %d is opened\n", subthread->id);
    for(; i < READ_WRITE_NUM/subthread->linear->thread_num; i++){
        if(subthread->run_queue[i].operation == 1){
            if (!subthread->linear->Insert(subthread->run_queue[i].key,(Value_t)subthread->run_queue[i].key)){   
                subthread->inserted ++;
            }
        }else{
            if(subthread->linear->Get(subthread->run_queue[i].key)!= NONE)
                // Get value
                cout << "Get value" <<endl;
        }
        //cout << "testing iteration " << i << endl;
    }
    cout << "kill thread take a long time " << endl;
    pthread_exit(NULL);
}


int main(int argc, char *argv[]){
  // Step 1: create (if not exist) and open the pool
	cout << "Testing Begin" <<endl;
    bool file_exist = false;
    if (FileExists(pool_name)) file_exist = true;

	if (!file_exist){
		PMEMobjpool *pop;
    	pop = pmemobj_create(pool_name, "Linear Hashing", 1024 * 1024 * 32UL, 0666);
    	if (pop == NULL) {
			perror(pool_name);
			exit(1);
		}
		    //size_t segment_number = (size_t)argv[1];
    size_t init_size = 1;
    cout << init_size << endl;
    PMEMoid root = pmemobj_root(pop, sizeof (LinearHash));
		struct LinearHash *rootp = (LinearHash*)pmemobj_direct(root);
    new (rootp) LinearHash();
		rootp->init_Linear(pop,rootp,init_size);
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
  clock_gettime(CLOCK_MONOTONIC, &start);	
	while(getline(&buf,&len,ycsb) != -1){
		if(strncmp(buf, "INSERT", 6) == 0){
			memcpy(key, buf+7, KEY_LEN-1);
			if (rootp->insert((const char *)&key,KEY_LEN,(const char *)&key,KEY_LEN,0,0))                      
			{
				inserted ++;
			}
		}
    if (inserted == READ_WRITE_NUM/2){
      break;
    }
	}
  clock_gettime(CLOCK_MONOTONIC, &finish);
	fclose(ycsb);
  
  printf("Load phase finishes: %d items are inserted \n", inserted);
  single_time = (finish.tv_sec - start.tv_sec) + (finish.tv_nsec - start.tv_nsec) / 1000000000.0;
  cout << "time usage is " <<single_time<< endl;

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
    if(operation_num == READ_WRITE_NUM/2){
      break;
    }
	}
	fclose(ycsb_read);
  cout << operation_num << endl;

	sub_thread* THREADS = (sub_thread*)malloc(sizeof(sub_thread)*thread_num);
    inserted = 0;
	
	cout << "Run phase begins: SEARCH/INSERTION ratio is 50/% 50/%" << endl;
    clock_gettime(CLOCK_MONOTONIC, &start);	
    for(int t = 0; t < thread_num; t++){
        THREADS[t].id = t;
        THREADS[t].linear = rootp;
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