// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_BRANCHDB_DB_MEMTABLE_H_
#define STORAGE_BRANCHDB_DB_MEMTABLE_H_

#include <string>
#include "branchdb/db.h"
#include "db/dbformat.h"
#include "db/skiplist.h"
#include "util/arena.h"

namespace branchdb {

class InternalKeyComparator;//dbformat.{h, cc}中定义(比较分为两个部分，userkey(升序排列) + tag(降序排列)
class Mutex; //这里没必要声明，注释了也没什么影响
class MemTableIterator;//由memtable.cc中定义(其实调用了SkipList中定义的迭代器）

//插入的格式见add函数定义中的格式
class MemTable {
 public:
  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.构造函数将refs_初始化为0
  explicit MemTable(const InternalKeyComparator& comparator);

  // Increase reference count.
  void Ref() { ++refs_; }

  // Drop reference count.  Delete if no more references exist.
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;//因为已经将析构函数声明为private，所以只能通过new创建MemTable,并且，只能调用Unref对我进行析构,以控制对象的撤销
    }            //而且见下，该类不支持复制构造函数和赋值操作符重载
  }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure.
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  //这里需要额外的同步：多线程不安全
  size_t ApproximateMemoryUsage(); //arena_占用的所有字节

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/format.{h,cc} module.
  Iterator* NewIterator(); // Iterator 是一个抽象基类在memtable.cc中由MemTableIterator实现，多线程不安全

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  //多线程不安全
  void Add(SequenceNumber seq, ValueType type, //caixin: typedef uint64_t SequenceNumber; enum ValueType
           const Slice& key,
           const Slice& value);

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // Else, return false.
  //多线程安全，不需要持有mutex
  bool Get(const LookupKey& key, std::string* value, Status* s); //LookupKey 在dbformat.{h,cc}中定义

 private:
  ~MemTable();  // Private since only Unref() should be used to delete it

  struct KeyComparator {//私有类成员,其实就是InternalKeyComparator加了一个（const char * ，const char*）的运算符重载
    const InternalKeyComparator comparator;//一个public const 成员
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }//用了InternalKeyComparator 的复制构造函数
    int operator()(const char* a, const char* b) const;  //memtable.cc定义，这个函数用于SkipList中的比较, 创建skiplist对象时需要用到该方法
  };
  friend class MemTableIterator;  //会访问本类中的Table类型
  friend class MemTableBackwardIterator;

  typedef SkipList<const char*, KeyComparator> Table; //Skiplist的模板为 template<typename Key, class Comparator>; Key需可相互赋值,Comparator需提供（key，key）操作符重载
                                                     //对于C++语法:Access control is applied uniformly to all names,  whether  the  names are  referred to from declarations or expressions.
  KeyComparator comparator_;//四个成员变量, 该成员应该为线程安全
  int refs_; //需要额外同步
  Arena arena_; //需要额外的同步
  Table table_; 

  // No copying allowed
  MemTable(const MemTable&);
  void operator=(const MemTable&);
};

}  // namespace branchdb

#endif  // STORAGE_BRANCHDB_DB_MEMTABLE_H_
