// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_BRANCHDB_INCLUDE_MTABLE_H_
#define STORAGE_BRANCHDB_INCLUDE_MTABLE_H_

#include <stdint.h>
#include <vector>
#include "branchdb/iterator.h"
#include "table/filter_block.h"
#include "branchdb/options.h"

namespace branchdb {

class Block; //用于读取block(data/index/metaindex block)
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class MtableCache;

// A Mtable is mutiple sorted maps from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
// 用于读取.sst文件
class Mtable {
 public:
  // Attempt to open the table that is stored in bytes [0..file_size)
  // of "file", and read the metadata entries necessary to allow
  // retrieving data from the table.
  //
  // If successful, returns ok and sets "*table" to the newly opened
  // table.  The client should delete "*table" when no longer needed.
  // 用户需要delete table,当不再使用时
  // If there was an error while initializing the table, sets "*table"
  // to NULL and returns a non-ok status.  Does not take ownership of
  // "*source", but the client must ensure that "source" remains live
  // for the duration of the returned table's lifetime.
  //
  // *file must remain live while this Table is in use.
  static Status Open(const Options& options,
                     RandomAccessFile* file, //为PosixRandomAccessFile*或PosixMmapReadableFile*(见函数PosixEnv::NewRandomAccessFile)
                     uint64_t index_offset_end,     //索引数据的结束偏移量
		     uint64_t index_offset,  //索引数据的偏移量，索引数据的大小可以通过file_size推断
                     Mtable** mtable);       //应该传入某个Table对象指针的地址，即：Table *a,传入 &a;

  ~Mtable(); //table.cc 中定义

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  Iterator* NewIterator(const ReadOptions&, uint64_t appendTimes) const;//实际类型为TwoLevelIterator（迭代器本身为线程不安全的，但是Table类型安全，理解原因）

 private:
  struct Rep; //私有的嵌套类成员
  Rep* rep_; //唯一的一个成员变量

  explicit Mtable(Rep* rep) { rep_ = rep; }
  static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);

  // Calls (*handle_result)(arg, ...) with the entry found after a call
  // to Seek(key).  May not make such a call if filter policy says
  // that key is not present.
  friend class MtableCache;
  //获得>=该key（参数k）的key/value对，传递给saver函数:(*saver)(arg, block_iter->key(), block_iter->value());
  Status InternalGet( //供TableCache调用
      const ReadOptions&, const Slice& key, uint64_t appendTimes,
      void* arg,
      bool (*handle_result)(void* arg, const Slice& k, const Slice& v));

  static Status Reopen(const Options& options, //供TableCache调用
                     RandomAccessFile* file,
                     uint64_t cache_index_offset,
                     uint64_t index_offset_end,  //索引数据的结束偏移量, 一般为fileCommonSize, 对于只存储一个sorted string的无洞的文件为文件的长度
	    	     uint64_t index_offset,      //索引数据的偏移量，索引数据的大小可以通过index_offset_end推断
		     const Mtable* mtable,
                     Mtable** newMtable);


  // No copying allowed
  Mtable(const Mtable&);
  void operator=(const Mtable&);
};

struct Mtable::Rep {
  ~Rep();
  void ReadFilterFromMem(const char* allData, const BlockHandle& filter_handle, uint64_t index_offset, const char **filter_data, FilterBlockReader **filter);

  Options options;
  Status status;
  RandomAccessFile* file;
  uint64_t cache_id;    //若option打开cache策略，每个sstable被打开时可以得到一个唯一的id(该id与一个block的offset可以组成一个唯一的key供cache使用)
  uint64_t index_offset_end;
  uint64_t index_offset;

  uint64_t num_sortedStrings; //下面三个数组,越末尾的元素越应先被读取
  std::vector<FilterBlockReader*> filters; //FilterBlockReader 用于读取特定filter(meta) block的类
  std::vector<const char*> filter_datas;  //当filter中绑定的数据为动态分配时，赋值（且有filter策略时被设置）

  std::vector<Block*> index_blocks;  //Block对象用于读取特定(只有一个)block(index block)的类, 对于点读取，先读后面的，在读前面的,读不管如何生成的都可以满足一个文件中后插入的先读取的逻辑

};

}  // namespace branchdb

#endif  // STORAGE_BRANCHDB_INCLUDE_TABLE_H_
