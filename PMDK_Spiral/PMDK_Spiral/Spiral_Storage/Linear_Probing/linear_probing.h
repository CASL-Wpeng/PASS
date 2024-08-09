#include <stddef.h>
#include <mutex>
#include <shared_mutex>
#include "../../common/pair.h"
#include "../../common/timer.h"
#include "../../common/hash.h"
#include "../../common/hash_api.h"
#ifndef LINEAR_HASH_H_
#define LINEAR_HASH_H_
#include "libpmemobj.h"

class LinearProbingHash : public hash_api {
  const float kResizingFactor = 2;
  const float kResizingThreshold = 0.95;
  public:
    PMEMobjpool *pop;
    LinearProbingHash(void);
    LinearProbingHash(size_t);
    ~LinearProbingHash(void);
    void init_Linear(PMEMobjpool *_pool, LinearProbingHash *linear,size_t init_size);
    bool Insert(Key_t&, Value_t);
    bool InsertOnly(Key_t&, Value_t);
    bool Delete(Key_t&);
    Value_t Get(Key_t&);
    double Utilization(void);
    size_t Capacity(void) {
      return capacity;
    }
    std::string hash_name() { return "Linear_Probing"; };

  bool find(const char *key, size_t key_sz, char *value_out, unsigned tid)
  {
    Key_t k = *reinterpret_cast<const Key_t *>(key);
    auto r = Get(k);

    return r;
  }

  bool insert(const char *key, size_t key_sz, const char *value,
              size_t value_sz, unsigned tid, unsigned t)
  {
    Key_t k = *reinterpret_cast<const Key_t *>(key);
    Insert(k, value);
    return true;
  }
  bool insertResize(const char *key, size_t key_sz, const char *value,
                    size_t value_sz)
  {
    Key_t k = *reinterpret_cast<const Key_t *>(key);
    return Insert(k, value);
  }
  bool update(const char *key, size_t key_sz, const char *value,
              size_t value_sz)
  {
    return true;
  }

  bool remove(const char *key, size_t key_sz, unsigned tid)
  {
    Key_t k = *reinterpret_cast<const Key_t *>(key);
    return Delete(k);
  }

  int scan(const char *key, size_t key_sz, int scan_sz, char *&values_out)
  {
    return scan_sz;
  }
  PMEMoid _dict;
  size_t capacity;
  Pair* dict;
  void resize(size_t);
  size_t getLocation(size_t, size_t, Pair*);

  size_t old_cap;
  Pair* old_dic;
  PMEMoid _old_dic;
  size_t size = 0;

  int resizing_lock = 0;
  int nlocks;
  int locksize;
  Timer timer;
  double breakdown = 0;
  PMEMoid _mutex;
  PMEMoid _old_mutex; /*old mutex lock*/
  PMEMrwlock *mutex;

};


#endif  // LINEAR_HASH_H_
