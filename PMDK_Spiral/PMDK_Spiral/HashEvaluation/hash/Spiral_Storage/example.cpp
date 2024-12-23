
#include "spiral_storage.h"
#include "allocator.h"

// pool path and name
static const char *pool_name = "/mnt/pmem0/pmem_hash.data";
// pool size
static const size_t pool_size = 1024ul * 1024ul * 1024ul * 10ul;

int main() {
  // Step 1: create (if not exist) and open the pool
  bool file_exist = false;
  if (FileExists(pool_name)) file_exist = true;
  Allocator::Initialize(pool_name, pool_size);

  // Step 2: Allocate the initial space for the hash table on PM and get the
  // root; we use Dash-EH in this case.
  Hash<uint64_t> *hash_table = reinterpret_cast<Hash<uint64_t> *>(
      Allocator::GetRoot(sizeof(SpiralStorage())));

  // Step 3: Initialize the hash table
  if (!file_exist)
  {
    // During initialization phase, allocate 64 segments for Dash-EH
    size_t sz = 64;
    size_t segment_number = sz >= 2 ? sz : 2;
    new (hash_table) SpiralStorage(
        segment_number, Allocator::Get()->pm_pool_);
  }
  else
  {
    new (hash_table) SpiralStorage();
  }

  // Step 4: Operate on the hash table
  // If using multi-threads, we need to use epoch for correct memory
  // reclamation; To make it simple, this example program only use one thread
  // but we still show how to use epoch mechanism; We enter into the epoch for
  // every 1024 operations to reduce the overhead; The following example inserts
  // 1024 * 1024 key-value pairs to the table, and then do the search and
  // delete operations

  // Insert
  int already_exists = 0;
  for (uint64_t i = 0; i < 1024; ++i) {
    // Enroll into the epoch, if using one thread, epoch mechanism is actually
    // not needed
    auto epoch_guard = Allocator::AquireEpochGuard();
    for (uint64_t j = 0; j < 1024; ++j) {
      auto ret = hash_table->Insert(i * 1024 + j, DEFAULT, true);
      if (ret == -1) already_exists++;
    }
  }

  std::cout << "Duplicate insert for first pass " << already_exists << std::endl;

  // Duplicate insert
  already_exists = 0;
  for (uint64_t i = 0; i < 1024; ++i) {
    // Enroll into the epoch, if using one thread, epoch mechanism is actually
    // not needed
    auto epoch_guard = Allocator::AquireEpochGuard();
    for (uint64_t j = 0; j < 1024; ++j) {
      auto ret = hash_table->Insert(i * 1024 + j, DEFAULT, true);
      if (ret == -1) already_exists++;
    }
  }

  std::cout << "Duplicate insert for second pass " << already_exists << std::endl;

  // Search
  uint64_t not_found = 0;
  Value_t value;
  for (uint64_t i = 0; i < 1024; ++i) {
    auto epoch_guard = Allocator::AquireEpochGuard();
    for (uint64_t j = 0; j < 1024; ++j) {
      if (hash_table->Get(i * 1024 + j) == false) {
        not_found++;
      }
    }
  }
  std::cout << "The number of keys not found: " << not_found << std::endl;

  // Delete
  for (uint64_t i = 0; i < 1024; ++i) {
    auto epoch_guard = Allocator::AquireEpochGuard();
    for (uint64_t j = 0; j < 1024; ++j) {
      hash_table->Delete(i * 1024 + j, true);
    }
  }

  return 0;
}