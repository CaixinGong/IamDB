// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)
//线程安全的类，当然获得的迭代器本身线程不安全

#ifndef STORAGE_BRANCHDB_DB_TABLE_CACHE_H_
#define STORAGE_BRANCHDB_DB_TABLE_CACHE_H_

#include <string>
#include <stdint.h>
#include "db/dbformat.h"
#include "branchdb/cache.h"
#include "branchdb/mtable.h"
#include "port/port.h"

namespace branchdb {

class Env;

class MtableCache {
 public:
  //capacity记录的是entry的个数（即key/value的个数）
  //即读.sst(.ldb)文件创建的Table*和打开的file的个数为charge
  MtableCache(const std::string& dbname, const Options* options, int entries);
  ~MtableCache();

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-NULL, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or NULL if no Table object underlies
  // the returned iterator.  The returned "*tableptr" object is owned by
  // the cache and should not be deleted, and is valid for as long as the
  // returned iterator is live.(即假设传递了tableptr以后得Table*,用户不能delete,
  // 因为 Iterator析构时会对引用减一 删除工作交给LRUCache
  // 获得Table的迭代器,当缓冲的Table对象不包括后面插入的内容，将利用缓冲的内容复制以及重新打开文件构建新的Table对象返回插入cache中（之前的对象从cache evict，当之前的引用计数为0时再析构）
  // 本次只读取文件的前面写入appendTimes次的数据，后面的不会读取
  Iterator* NewIterator(const ReadOptions& options,
                        uint64_t file_number,
			uint64_t index_offset_end,
		 	uint64_t index_offset,
                        uint64_t appendTimes,
                        Mtable** mtableptr = NULL);
  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  //直接使用Table提供给TableCache的私有函数：InternalGet。进行查阅key(internal key),并通过传递进的函数进行处理key/value。
  //当缓冲的Table对象不包括后面插入的内容，将利用缓冲的内容复制以及重新打开文件构建新的Table对象返回插入cache中（之前的对象从cache evict，当之前的引用计数为0时再析构）
  // 本次只读取文件的前面写入appendTimes次的数据，后面的不会读取
  Status Get(const ReadOptions& options,
             uint64_t file_number,
	         uint64_t index_offset_end,
             uint64_t index_offset,
             uint64_t appendTimes,
             const Slice& k,
             void* arg,
             bool (*handle_result)(void*, const Slice&, const Slice&));

  // Evict(逐出,指得是Cache中) any entry for the specified file number
  void Evict(uint64_t file_number);

 private:
  Env* const env_; //指针常量,env_指向的NewRandomAccessFile方法可能被多线程调用,只要该方法是线程安全的那么这个类就是多线程安全的
  const std::string dbname_; //数据库名
  const Options* options_;
  Cache* cache_; //线程安全的

  Status FindMtable(uint64_t file_number, uint64_t index_offset_end, uint64_t index_offset, Cache::Handle**);
};

}  // namespace branchdb

#endif  // STORAGE_BRANCHDB_DB_TABLE_CACHE_H_
