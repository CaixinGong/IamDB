// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "branchdb/cache.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace branchdb {

Cache::~Cache() {
}

namespace {

// LRU cache implementation

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
// LRUHandle为该双向循环链表中的节点
struct LRUHandle {
  void* value;    //实际要存储的东西
  void (*deleter)(const Slice&, void* value); //当本LRUHandle要从cache中剔除时(refs变为0)需调用的函数，如回收value(如Block*)的资源
  LRUHandle* next_hash;  //组织hash桶的单向链表用于快速定位
  LRUHandle* next;		 //next prev组织双向链表用于支持LRU算法
  LRUHandle* prev;
  size_t charge;      // TODO(opt): Only allow uint32_t?
  size_t key_length;
  uint32_t refs;    //每个持有该LRUHandle对象的指针的用户(对于LRUCache来说的用户:ShardedLRUCache)都使refs++; 若无用户,refs = 1(用于LRUCache的析构函数中)
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
  char key_data[1];   // Beginning of key(相对于用 char * key_data 会节省空间,且由LRUCache::Insert函数可知这样更加易于内存管理)

  Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) { //这针对上面的注释:对于没有key的存储不需要LRU Cache，只是存储在Hash table中,temporary Handle object,暂时没有见到这个的使用
      return *(reinterpret_cast<Slice*>(value));
    } else {
      return Slice(key_data, key_length);
    }
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
// hash 表:平均每个桶LRUHandle * 的个数 <= 1,用以快速定位
// 该类的所有函数不用加锁，因为被LRUCache类使用，LRUCache使用了锁
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(NULL) { Resize(); } //Resize重新分配空间，length_初始时为4
  ~HandleTable() { delete[] list_; }

  LRUHandle* Lookup(const Slice& key, uint32_t hash) { //通过hash table寻找key和hash都相等的LRUHandle*,不存在返回NULL
    return *FindPointer(key, hash);
  }

  //根据h->hash找到相应的桶进行插入：若不存在相等的key/hash, 尾插, 并返回NULL
  //若存在相等的,用*h替换该LRUHandle对象在链表中的位置,返回被替换的LRUHandle的指针(LRUHandle*)(并不直接析构，会由用户Realse Handle而析构)
  LRUHandle* Insert(LRUHandle* h) {
    // 根据hash定位到桶中, 再在相应的桶中,找hash和key都相等的，若存在返回指向该LRUHandle*的"指针"(指向上一个节点的next_hash),
    //若不存在返回链表中最后一个节点的next_hash的地址(LRUHandle **),该指针指向的值,LRUHandle*为NULL(见leveldb.vsd图示)
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    //这两句从hash table中移除当前的(不析构也不使引用-1,引用-1在LRUCache::Insert中进行),使其指向下一个（单向链表）
    h->next_hash = (old == NULL ? NULL : old->next_hash);
    *ptr = h;//插入该节点,修改"查找到的节点的上一个节点的next_hash"指向 h
    if (old == NULL) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average(平均) linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }
  //从桶中删除key/hash都相等的(若不存在直接返回NULL)，返回从桶中删除的LRUHandle对象的指针，即LRUHandle*(不析构,由用户不使用时解引用)
  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);//寻找key和hash都相等的LRUHandle*的指针ptr,若不存在*ptr为NULL
    LRUHandle* result = *ptr;
    if (result != NULL) {
      *ptr = result->next_hash;//删除当前的，使其指向下一个（单链表）
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;//hash表桶的数量, 是2的幂
  uint32_t elems_;//元素的数量
  LRUHandle** list_;//hash表list_ = new LRUHandle*[length_];

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash(都要相等).  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  // 根据hash定位到桶中, 再在相应的桶中,找hash和key都相等的，若存在返回指向该LRUHandle*的"指针"(指向上一个节点的next_hash),
  //                                   若不存在返回链表中最后一个节点的next_hash的地址(LRUHandle **),该指针指向的值,LRUHandle*为NULL(见leveldb.vsd图示)
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != NULL &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) { //注意hash有32位，而用来选择桶的只有低的的几位
      ptr = &(*ptr)->next_hash;                              //key/hash相等的才会返回该LRUHandle*的地址,否则向后寻找,若没有相等的则返回NULL
    }
    return ptr;//注意ptr不可能为NULL，*ptr可能为NULL，也可能不为NULL
  }

  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) { //构造函数中使用的length_的值为0,不执行
      LRUHandle* h = list_[i];
      while (h != NULL) {
        LRUHandle* next = h->next_hash;//保存桶中下一个的指针(while循环使用)
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];//需重新计算映射到哪一个桶
        h->next_hash = *ptr;//头插法插入该桶(为桶中的第一个时，*ptr为NULL),桶中的先后顺序会被打乱
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard(碎片) of sharded(分片的) cache. (内部使用了互斥量满足多线程安全)
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter. 一下四个public方法都加锁全部作用域
  Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);

 private:  //私有函数不用加锁，因为由public函数调用，而public函数全部加锁
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* e);
  void Unref(LRUHandle* e);

  // Initialized before use.（容量）
  size_t capacity_;

  // mutex_ protects the following state.
  port::Mutex mutex_; //用于保护一个LRUCache对象只能一个线程使用的场合
  size_t usage_; //已经使用的容量，当usage_ > capacity_需选择最旧页进行驱逐

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.(因为为双向循环链表)
  LRUHandle lru_; //该双向链表的头节点

  HandleTable table_;
};

LRUCache::LRUCache()//mutex_，lru_，table_调用了默认构造函数
    : usage_(0) {
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
}

//显式删除所有的LRUHandle(lru_不需要，处于栈中)
LRUCache::~LRUCache() {
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    LRUHandle* next = e->next;
    assert(e->refs == 1);  // Error if caller has an unreleased handle,即持有该LRUHandle指针的用户全部已经release完毕了,才可析构
    Unref(e);
    e = next;
  }
}

//私有函数:减少引用计数，若已无引用:则执行deleter函数回收关于key和value的空间; free LRUHandle* 本身空间。
void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs <= 0) {
    usage_ -= e->charge;
    (*e->deleter)(e->key(), e->value);
    free(e);
  }
}

//私有函数:从双向链表中删除一个结点 
void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

//私有函数:在lru_前面（.prev最新）插入(为双向链表,改4个指针)
void LRUCache::LRU_Append(LRUHandle* e) {
  // Make "e" newest entry by inserting just before lru_
  e->next = &lru_;
  e->prev = lru_.prev;
  e->prev->next = e;
  e->next->prev = e;
}

//返回结点的用户增加,该节点的refs++
//将该结点放置到双向链表最新位置
//若没有则返回NULL
Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);//构造函数加锁，析构函数解锁(Insert 函数结束时)
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != NULL) {
    e->refs++; //用户增加(LRUCahe的用户)
    LRU_Remove(e);//双向链表中移除
    LRU_Append(e);//双向链表中插入到最新的位置(lru_.prev，以满足LRU cache管理算法的要求
  }
  return reinterpret_cast<Cache::Handle*>(e);//强制类型转换
}

//当用户不使用该LRUHandle*指向的结点时调用以减少引用计数
void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);//加锁
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

//deleter函数为当对应的handle无引用时回收相关空间(关于key和value的空间)
//构造新的插入，若之前存在则替换之前的:将之前的从table_和lru_中移除，并将其引用计数-1; 新插入的引用计数初始化为2.
Cache::Handle* LRUCache::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) {
  //加锁(只能有一个线程可同时执行),Insert函数结束时自动解锁 
  MutexLock l(&mutex_); //构造函数加锁，析构函数解锁(Insert 函数结束时)

  //根据参数构造LRUHandle对象
  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle)-1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->refs = 2;  // One from LRUCache, one for the returned handle
  memcpy(e->key_data, key.data(), key.size());
  //在lru_前插入(通过lru_.prev访问最新插入的，lru_.next访问最老的结点)
  LRU_Append(e);

  usage_ += charge; //使用的空间需要加

  //找到相应的桶进行插入,若不存在相等的key/value,尾插,返回NULL; 若存在相等的,则返回被替换的LRUHandle的指针(LRUHandle*)
  LRUHandle* old = table_.Insert(e);
  if (old != NULL) {
    LRU_Remove(old); //从双向链表中删除该节点
    Unref(old);      //减少引用计数，若已无引用:则执行deleter函数，回收相关空间
  }

  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;  //找到最老结点
    LRU_Remove(old);             //从双向链表中删除该老节点
    table_.Remove(old->key(), old->hash);//从hash table中删除该老结点
    Unref(old);//减少引用计数，若已无引用:则执行deleter函数，回收相关空间
  }

  return reinterpret_cast<Cache::Handle*>(e); //强制类型转换
}

//对应于inset，这里为从LRUCache中擦除key，hash值相等的节点（如果存在的话）
void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Remove(key, hash);//从hash table中删除该节点
  if (e != NULL) {
    LRU_Remove(e);//从双向链表中删除该节点
    Unref(e); //减少引用计数，若已无引用:则执行deleter函数，回收相关空间，注意一定会从LRUCache的数据结构中移除
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards]; //kNumShardBits 为16,分配多个LRUCache的原因是: 可通过下面两个static函数进行选择，从而加快多线程的访问速率
                               //LRUCache里的互斥访问粒度很粗，这样实现简单。通过以上两个机制，使得实现简单并且锁的粒度细
  port::Mutex id_mutex_;//Mutex为默认属性创建的互斥量(对 last_id_进行同步)
  uint64_t last_id_;// 修改last_id_时进行了互斥控制

  static inline uint32_t HashSlice(const Slice& s) { //计算hash值，参数为key
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) {//将hash右移(32-kNumShardBits(默认为4))位, 剩下高kNumShardBits位,用于选择LRUCache,即Shard( HashSlice(key) ) 生成的值用于选择
    return hash >> (32 - kNumShardBits);
  }

 public:
  explicit ShardedLRUCache(size_t capacity)
      : last_id_(0) {   //每个LRUCache调用了默认构造函数 ,id_mutex_调用默认构造函数生成了一个默认属性的互斥量(Mutex类进行了极简单的封装) 
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  virtual ~ShardedLRUCache() { }

  //charge表示value消耗多少容量，deleter表示要彻底删除这个Insert要对key，value做的清理
  //用户需要对返回值(当不再使用时)调用Release
  virtual Handle* Insert(const Slice& key, void* value, size_t charge, 
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  virtual Handle* Lookup(const Slice& key) { //用户需要对返回值(当不再使用时)调用Release
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  virtual void Release(Handle* handle) { //用户不再通过该指针引用该对象使用
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  //从LRUCache中的双向链表和hash表都删除，并减少一个引用（若减少到0即没有用户再使用）测定删除对应的LRUHandle对象
  virtual void Erase(const Slice& key) { 
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  virtual uint64_t NewId() {
    MutexLock l(&id_mutex_); //构造函数对互斥量加锁, 析构函数对互斥量解锁
    return ++(last_id_);
  } //跳出作用域，调用MutexLock的析构函数，进行了解锁
};

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) {
  return new ShardedLRUCache(capacity);
}

}  // namespace branchdb
