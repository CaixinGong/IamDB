// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_BRANCHDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_BRANCHDB_TABLE_FILTER_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>
#include "branchdb/slice.h"
#include "util/hash.h"

namespace branchdb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.（通过finish返回）
//
// The sequence of calls to FilterBlockBuilder must match the regexp:(注意这段注释：如何使用)
//      (StartBlock AddKey*)* Finish
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*); //如用布隆过滤器,参数是NewBloomFilterPolicy()函数返回的

  //当调用了构造函数后调用,参数为0(此时什么也没做)
  //和当每写了一个data Block + type(压缩策略) + crc32至.sst文件时调用，参数为已写的data block的末尾
  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);//当data block每添加一条记录时调用
  Slice Finish();

 private:
  void GenerateFilter();

  const FilterPolicy* policy_;    //提供过滤器的方法,为一个抽象基类，实际类型为bloom filter
  std::string keys_;              // Flattened key contents(所有待创建filter的keys，连续存储,会被清空)
  std::vector<size_t> start_;     // Starting index in keys_ of each key(key_中每个key的index, 会被清空)
  std::string result_;            // Filter data computed so far(不会清空，一直添加), 最后保存着整个filter block的内容，包括最后的尾端信息
  std::vector<Slice> tmp_keys_;   // policy_->CreateFilter() argument(由上面keys_ 和start_生成的创建当前的filter时使用)
  std::vector<uint32_t> filter_offsets_;

  // No copying allowed
  FilterBlockBuilder(const FilterBlockBuilder&);
  void operator=(const FilterBlockBuilder&);
};

class FilterBlockReader { //采用默认的析构函数
 public:
 // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);//contents为整个filter block的所有内容
  //输入block offset为一个data block 起始(即BlockHandle 中的offset_)
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);
  size_t size() const { return size_; };

 private:
  const FilterPolicy* policy_;   //提供过滤器的方法,下面四个与filter block的格式有关
  const char* data_;    // Pointer to filter data (at block-start)
  const char* offset_;  // Pointer to beginning of offset array (at block-end)
  size_t size_;
  size_t num_;          // Number of entries in offset array
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)
};

}

#endif  // STORAGE_BRANCHDB_TABLE_FILTER_BLOCK_H_
