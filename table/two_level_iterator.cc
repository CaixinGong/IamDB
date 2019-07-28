// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "branchdb/mtable.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace branchdb {

namespace {//匿名命名空间

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

class TwoLevelIterator: public Iterator {
 public:
  TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options);

  virtual ~TwoLevelIterator();

  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Next();
  virtual void Prev();

  //Valid函数，即data_iter_.Valid()
  virtual bool Valid() const {
    return data_iter_.Valid();
  }
  virtual Slice key() const {
    assert(Valid());
    return data_iter_.key();
  }
  virtual Slice value() const {
    assert(Valid());
    return data_iter_.value();
  }
  virtual Status status() const {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != NULL && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  BlockFunction block_function_;
  void* arg_;
  const ReadOptions options_;
  Status status_;
  IteratorWrapper index_iter_; //第一层
  IteratorWrapper data_iter_; // May be NULL(第二层 当为NULL时当前状态为invalid)
  // If data_iter_ is non-NULL, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_(在InitDataBlock 函数中).
  std::string data_block_handle_;
};

TwoLevelIterator::TwoLevelIterator(
    Iterator* index_iter,//第一层的迭代器(在遍历Table时为index block的迭代器)
    BlockFunction block_function,
    void* arg, //在遍历table时，为Table *
    const ReadOptions& options)
    : block_function_(block_function), //用于获取第二层迭代器的函数指针(在遍历Table时，为&BlockReader函数)
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(NULL) {
}

TwoLevelIterator::~TwoLevelIterator() {
}

void TwoLevelIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);//获得第一个key>=taget的记录
  //若data_iter_为NULL或index_iter_.value()返回的BlockHandle不等于data_block_handle_，根据index_iter_设置data_block_handle_和data_iter_
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.Seek(target);//data_iter_定位到正确位置(注意若当前data block非空一定可以定位到的)获得>=target即可
  //若当前data_iter_为无效(当data_iter_不空且valid时什么也不执行):跳过该datablock，定位到第一个非空block的第一个记录
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  //若data_iter_为NULL或index_iter_.value()返回的BlockHandle不等于data_block_handle_，根据index_iter_设置data_block_handle_和data_iter_
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.SeekToFirst();
  //若data_iter_为NULL或index_iter_.value()返回的BlockHandle不等于data_block_handle_，根据index_iter_设置data_block_handle_和data_iter_
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  //若data_iter_为NULL或index_iter_.value()返回的BlockHandle不等于data_block_handle_，根据index_iter_设置data_block_handle_和data_iter_
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.SeekToLast();
  //若当前data_iter_为无效(当data_iter_不空且valid时什么也不执行):跳过该datablock，定位到上面第一个非空block的最后一条记录
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();//data_iter不空且valid什么也不做，否则跳过下面的空datablock，定位到非空block的第一个记录
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  //若当前data_iter_为无效(当data_iter_不空且valid时什么也不执行):跳过该datablock，定位到上面第一个非空block的最后一条记录
  SkipEmptyDataBlocksBackward();
}

//本函数主要是Next函数中使用(data_iter_遍历完其data block，重新设置data_iter_), 当然Seek中也可能使用(因为Index block中的key可能会大于整个data block的内容)
//若当前data_iter_为无效(当data_iter_不空且valid时什么也不执行):跳过该datablock，定位到第一个非空block(空时将不valid)的第一个记录
  //其他时候往下遍历直到找到非空datablock（valid）或整个.sst文件遍历完毕（即index_iter_.valid()为false：invalid）
  //找到非空datablock时，data_iter_.SeekToFirst()
void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == NULL || !data_iter_.Valid()) {//当data_iter_不空且valid时什么也不执行
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(NULL);//使TwoLevelIterator为Valid()为false
      return;
    }
    index_iter_.Next();
    InitDataBlock();//若data_iter_为NULL，设置data_iter_
    if (data_iter_.iter() != NULL) data_iter_.SeekToFirst();
  }
}

//本函数主要在prev中使用(data_iter_遍历完其data block，重新设置data_iter_)
//若当前data_iter_为无效(当data_iter_不空且valid时什么也不执行):跳过该datablock，定位到上面第一个非空block(空时将不valid)的最后一条记录
  //跳过上面空的datablock,定位到第一个非空block的最后一条记录
  //当data_iter_不空且valid时什么也不执行
void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == NULL || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(NULL);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != NULL) data_iter_.SeekToLast();
  }
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != NULL) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);//设置data_iter_:析构原来的迭代器，并重新设置iter_,和其他所有成员变量(valid_ key_),
}

//若data_iter_为NULL或index_iter_.value()返回的BlockHandle不等于data_block_handle_，根据index_iter_设置data_block_handle_和data_iter_
void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_.Valid()) {
    SetDataIterator(NULL);//使TwoLevelIterator为Valid()为false
  } else {
    Slice handle = index_iter_.value();//获得data block的offset_和size_的varint64的编码
    if (data_iter_.iter() != NULL && handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything(因为这里是init 才会调用)
    } else {
      Iterator* iter = (*block_function_)(arg_, options_, handle);//可以为BlockReader函数：获得data_block的迭代器
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

}  // namespace

Iterator* NewTwoLevelIterator(
    Iterator* index_iter, //第一层迭代器
    BlockFunction block_function, //生成第二层迭代器的函数
    void* arg,//下面两为block_function的参数
    const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace branchdb
