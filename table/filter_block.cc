// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "branchdb/filter_policy.h"
#include "util/coding.h"

namespace branchdb {

// See doc/table_format.txt for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg; //2KB

//剩下的5个私有成员调用了默认构造函数
FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {
}
//参数block_offset为"将"要创建的下一个data block的起始偏移量
//功能为：为block_offset"之前"所add的key(不包括已经创建过的,即调用过StartBlock函数的)创建filter(所以若参数为0什么也不做)
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  uint64_t filter_index = (block_offset / kFilterBase); //filter_index为要创建的filter的个数
  assert(filter_index >= filter_offsets_.size()); // filter_offsets_.size()为已经创建的filter的个数
  while (filter_index > filter_offsets_.size()) { //注意这里为while循环, 若data block为4K，meta block为2k，这里一般循环两次
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size()); //第一个元素为0
  keys_.append(k.data(), k.size());
}

Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) { //StartBlock()后没有addkey的为空, 此处
    GenerateFilter();
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  PutFixed32(&result_, array_offset); //可见一个filter block一定有下面5个字节，offset of the beginning of offset array
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result, 隐式类型转换 size_t -> char
  return Slice(result_);
}

//私有函数
//从key_ 和start_ 的提取出temp_keys,生成filter并将其内容append到result_尾部
//push_back 一个filter offset，若start_为空则push_back 上一个filter offset 
//清空 key_ 和start_
void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size(); //start_在本函数的末尾会被clear
  if (num_keys == 0) {//若data block为4k，meta block为2k，这里一般第i(i为奇数，从0开始计数)个meta block会执行
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());//第n(n>1)次为第一次的result_的末尾(默认n可为2)
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation //为了下面的计算简单而加入的
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i+1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  filter_offsets_.push_back(result_.size()); //可见第0个offset存储着0; result_(包括迄今为止所有的filter 内容)不会清空，一直添加
  policy_->CreateFilter(&tmp_keys_[0], num_keys, &result_); //注意转换vector<type> 成 type* 的方法(见浏览器书签解释)

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)//构造函数 ,contents为整个filter block的所有内容
    : policy_(policy),
      data_(NULL),
      offset_(NULL),
      size_(0),
      num_(0),
      base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  size_ = n;
  base_lg_ = contents[n-1]; //最后一个字节,char类型
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5); //偏移量数组的起始偏移量
  if (last_word > n - 5) return;//索引到本filter block后面去了，发生格式错误
  data_ = contents.data();
  offset_ = data_ + last_word; // Pointer to beginning of offset array (at block-end)
  num_ = (n - 5 - last_word) / 4; //过滤器的个数
}

//输入block offset为一个data block 起始(即BlockHandle 中的offset_),key为需要判断的key
//根据block offset可获得所需的filter，再判断key根据该filter是否在集合中，若在则返回true
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index*4);//取出地址指向的值，即offset_[index]
    uint32_t limit = DecodeFixed32(offset_ + index*4 + 4);//取offset[index+1](若为最后一个取出的是array_offset)
    if (start <= limit && limit <= (offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);//得到整个filter的内容（包括后面的一个字节，存储着k_）
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}
