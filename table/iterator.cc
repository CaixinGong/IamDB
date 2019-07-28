// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "branchdb/iterator.h"

namespace branchdb {

Iterator::Iterator() {
  cleanup_.function = NULL;
  cleanup_.next = NULL;
}

Iterator::~Iterator() {
  if (cleanup_.function != NULL) { //调用头节点(栈中)注册的函数
    (*cleanup_.function)(cleanup_.arg1, cleanup_.arg2);
    for (Cleanup* c = cleanup_.next; c != NULL; ) {//调用非头节点(堆中)注册的函数
      (*c->function)(c->arg1, c->arg2);
      Cleanup* next = c->next;
      delete c; //delete已经调用过的非头节点
      c = next;
    }
  }
}

//注册函数func, func在Iterator析构时使用
void Iterator::RegisterCleanup(CleanupFunction func, void* arg1, void* arg2) {
  assert(func != NULL);
  Cleanup* c;
  if (cleanup_.function == NULL) {//第一次使用头节点注册调用函数
    c = &cleanup_;
  } else {                        //第二次及以后调用new一个结点注册调用的函数
    c = new Cleanup; //非头节点,动态分配
    c->next = cleanup_.next;//头插(cleanup_作为头节点)
    cleanup_.next = c;
  }
  c->function = func;
  c->arg1 = arg1;
  c->arg2 = arg2;
}

namespace {  //匿名命名空间，只可在本文件内使用，供下面两个函数使用，间接给别的文件使用
class EmptyIterator : public Iterator {
 public:
  EmptyIterator(const Status& s) : status_(s) { }
  virtual bool Valid() const { return false; }
  virtual void Seek(const Slice& target) { }
  virtual void SeekToFirst() { }
  virtual void SeekToLast() { }
  virtual void Next() { assert(false); }
  virtual void Prev() { assert(false); }
  Slice key() const { assert(false); return Slice(); }
  Slice value() const { assert(false); return Slice(); }
  virtual Status status() const { return status_; }
 private:
  Status status_;
};
}  // namespace

Iterator* NewEmptyIterator() {
  return new EmptyIterator(Status::OK());
}

Iterator* NewErrorIterator(const Status& status) {
  return new EmptyIterator(status);
}

}  // namespace branchdb
