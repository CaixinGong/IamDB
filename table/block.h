// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_BRANCHDB_TABLE_BLOCK_H_
#define STORAGE_BRANCHDB_TABLE_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include "branchdb/iterator.h"

namespace branchdb {

struct BlockContents;
class Comparator;

//用于读取Block(data/index/metaindex block都使用本格式)
class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(const BlockContents& contents); //BlockContents 的存储的为整个block的内容

  ~Block();

//除构造和析构，只提供这两个接口

  size_t size() const { return size_; } //返回该block的大小
  //这个迭代器可以实现常用的很多操作，包括随机读(多个seek操作)，顺序读任意一条记录
  Iterator* NewIterator(const Comparator* comparator);

 private:
  uint32_t NumRestarts() const; //返回contents中的restart点的个数

  friend class Mtable; //将访问下面的私有成员
  const char* data_; //整个block的在内存中的首地址(指向BlockContents中的空间)
  size_t size_;      //整个block的大小
  uint32_t restart_offset_;     // Offset in data_ of restart array
  bool owned_;                  // Block owns data_[]

  // No copying allowed
  Block(const Block&);
  void operator=(const Block&);

  class Iter; //私有类成员，其定义见block.cc
};

}  // namespace branchdb

#endif  // STORAGE_BRANCHDB_TABLE_BLOCK_H_
