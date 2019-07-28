// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_BRANCHDB_TABLE_ITERATOR_WRAPPER_H_
#define STORAGE_BRANCHDB_TABLE_ITERATOR_WRAPPER_H_

namespace branchdb {

// A internal wrapper class with an interface similar to Iterator that
// caches the valid() and key() results for an underlying iterator.(这样也就不需要调用Iterator::key()虚函数了,那么cache局部性更好,在MergingIterator经常比较显然较为有用)
// This can help avoid virtual function calls(不需要一定是对象的指针或引用调用) and also gives better
// cache locality.
class IteratorWrapper {
 public:
  IteratorWrapper(): iter_(NULL), valid_(false) { }  //key_调用了默认构造函数
  explicit IteratorWrapper(Iterator* iter): iter_(NULL) { //接收抽象基类指针Iterator*的构造函数
    Set(iter);
  }
  ~IteratorWrapper() { delete iter_; }
  Iterator* iter() const { return iter_; }

  // Takes ownership of "iter" and will delete it when destroyed, or
  // when Set() is invoked again.(即传递进来的iter变量是动态分配的，且IteratorWrapper内部进行delete，不用外部操心)
  // 析构原来的迭代器，并重新设置iter_,和其他所有成员变量(valid_ key_),
  // 为新建iterator_wrapper数组(调用默认构造函数)时调用
  void Set(Iterator* iter) {
    delete iter_;
    iter_ = iter;
    if (iter_ == NULL) {
      valid_ = false;
    } else {
      Update();//设置valid_ key_
    }
  }


  // Iterator interface methods
  bool Valid() const        { return valid_; }
  Slice key() const         { assert(Valid()); return key_; }
  Slice value() const       { assert(Valid()); return iter_->value(); }
  // Methods below require iter() != NULL
  Status status() const     { assert(iter_); return iter_->status(); }
  void Next()               { assert(iter_); iter_->Next();        Update(); }
  void Prev()               { assert(iter_); iter_->Prev();        Update(); }
  void Seek(const Slice& k) { assert(iter_); iter_->Seek(k);       Update(); }
  void SeekToFirst()        { assert(iter_); iter_->SeekToFirst(); Update(); }
  void SeekToLast()         { assert(iter_); iter_->SeekToLast();  Update(); }

 private:
  void Update() {//设置valid_和key_
    valid_ = iter_->Valid();
    if (valid_) {
      key_ = iter_->key();
    }
  }

  Iterator* iter_;
  bool valid_;
  Slice key_;
};

}  // namespace branchdb

#endif  // STORAGE_BRANCHDB_TABLE_ITERATOR_WRAPPER_H_
