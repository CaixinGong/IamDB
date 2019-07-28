// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/mtable_cache.h"

#include "db/filename.h"
#include "branchdb/env.h"
#include "branchdb/mtable.h"
#include "util/coding.h"

namespace branchdb {
//file_number作为key, MtableAndFile*作为value
struct MtableAndFile { 
  RandomAccessFile* file;
  Mtable* mtable;
};

//传入Cache的deleter函数: 当cache中的元素（LRUHandle)对应的handle引用计数为0时(注意在cache中引用计数就有1了,被evict减1)回收相关空间(关于key和value的空间)
static void DeleteEntry(const Slice& key, void* value) { 
  MtableAndFile* tf = reinterpret_cast<MtableAndFile*>(value);
  delete tf->mtable;
  delete tf->file;
  delete tf;
}

//对得到的Handle*指针arg2进行release操作 ( arg1->Rlease(arg2) )
static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

//构造函数
MtableCache::MtableCache(const std::string& dbname,
                       const Options* options,
                       int entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {
}

//析构函数
MtableCache::~MtableCache() {
  delete cache_;
}

//根据key（file_number）查阅cache以返回handle，若cache中无，则利用Table::open等进行构造一个key/value对，并插入cache,获得handle
//其中cache 是以file_number 为key，TableAndFile*作为value的cache其中TableAndFile包括打开的文件和Table*
//注意的一点是，当缓冲的Table对象不包括后面插入的内容，将利用缓冲的内容复制以及重新打开文件构建新的Table对象返回插入cache中（之前的对象从cache evict，当之前的引用计数为0时再析构）
Status MtableCache::FindMtable(uint64_t file_number, uint64_t index_offset_end, uint64_t index_offset,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number); //file_number作为key, MtableAndFile*作为value
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);      //从cache中寻找,并返回handle*（注意使用完后,需要调用cache_->Release(Handle*) 
  if(*handle !=NULL) {    //若从cache中找到,对两种情况"全在内存中"和"部分在内存中"的判断 
    std::string fname = MtableFileName(dbname_, file_number);        //fname可能为sunLevel/000005.ldb
    const Mtable* mtable = reinterpret_cast<MtableAndFile*>(cache_->Value(*handle))->mtable;
    RandomAccessFile* file = NULL;
    Mtable * newMtable = NULL;
    uint64_t cache_index_offset_end = mtable->rep_->index_offset_end;
    uint64_t cache_index_offset = mtable->rep_->index_offset;
    assert(cache_index_offset_end == index_offset_end);
    if(cache_index_offset > index_offset) { //部分在内存中(对于全部在内存中无需其他处理)
      s = env_->NewRandomAccessFile(fname, &file);                    //file可能被多线程访问,需重新打开，因为若使用之前的可能会被关闭
      if (s.ok()) {                                                   //创建Table对象, table指向该对象
        s = Mtable::Reopen(*options_, file, cache_index_offset,  index_offset_end, index_offset, mtable, &newMtable);
      }
      cache_->Release(*handle);                  //注意,对旧的handle的引用减一
      if (!s.ok()) {
        assert(newMtable == NULL);
        delete file;
        // We do not cache error results so that if the error is transient,
        // or somebody repairs the file, we recover automatically.
      } else {                                                         //插入LRU策略的cache
        MtableAndFile* tf = new MtableAndFile;
        tf->file = file;
        tf->mtable = newMtable;
        *handle = cache_->Insert(key, tf, 1, &DeleteEntry);     //会将原来的从cache中evict，引用减1， 插入新的，引用初始为2
								//charge为1,说明cache是以key/value的个数作为capacity的charge
      }
    } 
    //对于全在内存中的情况(和超出本次读取的信息也在内存的情况)，无需其他处理
  }
  else{     //若未找到
    std::string fname = MtableFileName(dbname_, file_number);        //fname可能为sunLevel/000005.ldb
    RandomAccessFile* file = NULL;
    Mtable* mtable = NULL;
    s = env_->NewRandomAccessFile(fname, &file);                    //file可能被多线程访问
    if (s.ok()) {                                                   //创建Table对象, table指向该对象
      s = Mtable::Open(*options_, file, index_offset_end, index_offset, &mtable);
    }

    if (!s.ok()) {
      assert(mtable == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {                                                         //插入LRU策略的cache
      MtableAndFile* tf = new MtableAndFile;
      tf->file = file;
      tf->mtable = mtable;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);             //charge为1,说明cache是以key/value的个数作为capacity的charge
    }
  }
  return s;
}
//会加入Cache(不管readoption的设置如何一定为加入table cache，而block cache可选,这里选上了)
//根据file_number调用FindMTable(先查cache，若没有再进行Table::open构造，并插入cache）以得到handle*
//然后利用handle得到Table*以获得table的迭代器返回(该迭代器可能使用了DataBlock的cache，默认打开）
// 假设传递了tableptr以后该迭代器对应的Table*,用户不能delete,因为 Iterator析构时会对引用减一
// 删除工作交给LRUCache
Iterator* MtableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t index_offset_end,
								  uint64_t index_offset,
                                  uint64_t appendTimes,
                                  Mtable** mtableptr) {
  if (mtableptr != NULL) {
    *mtableptr = NULL;
  }

  Cache::Handle* handle = NULL;
  Status s = FindMtable(file_number, index_offset_end, index_offset, &handle); //有查TableCache的步骤
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Mtable* mtable = reinterpret_cast<MtableAndFile*>(cache_->Value(handle))->mtable;
  Iterator* result = mtable->NewIterator(options, appendTimes); //返回table的迭代器(该迭代器可能使用了DataBlock的cache，默认打开）
  result->RegisterCleanup(&UnrefEntry, cache_, handle); //注册函数：迭代器析构时,对handle的引用减一(若引用变为0,析构key，value(table*)以及handle本身)
  if (mtableptr != NULL) {
    *mtableptr = mtable;
  }
  return result;
}

//不通过迭代器，直接使用Table提供给TableCache的私有函数：InternalGet。进行查阅key,并通过传递进的函数进行处理
//注意不管是FindMTable还是InternalGet都使用了table cache或者block cache
Status MtableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t index_offset_end,
                       uint64_t index_offset,
                       uint64_t appendTimes,
                       const Slice& k,
                       void* arg,
                       bool (*saver)(void*, const Slice&, const Slice&)) {
  Cache::Handle* handle = NULL;
  Status s = FindMtable(file_number, index_offset_end, index_offset, &handle); //在table cache中查找获得handle*
  if (s.ok()) {
    Mtable* t = reinterpret_cast<MtableAndFile*>(cache_->Value(handle))->mtable;
    s = t->InternalGet(options, k, appendTimes, arg, saver);//调用Table提供给TableCache的私有函数(会查阅block cache)
    cache_->Release(handle);                  //对handle的引用减一
  }
  return s;
}

//直接从TableCache中擦除某个.sst(.ldb)的即key/value记录
void MtableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace branchdb
