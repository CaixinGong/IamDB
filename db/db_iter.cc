// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"

#include "db/filename.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "branchdb/env.h"
#include "branchdb/iterator.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace branchdb {

#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

namespace {//匿名命名空间

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
// 即从用户的角度应该有的迭代器的功能（包括对删除或者更新的记录的正确的查找; 快照即seqeunce number功能）
class DBIter: public Iterator {
 public:
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  enum Direction {
    kForward,
    kReverse
  };

  DBIter(DBImpl* db, const Comparator* cmp, Iterator* iter, SequenceNumber s,
         uint32_t seed)
      : db_(db),
        user_comparator_(cmp),
        iter_(iter),
        sequence_(s),//status, saved_key(string),saved_value_(string)调用了默认构造函数
        direction_(kForward),
        valid_(false),
        rnd_(seed),//Random类型：用于产生随机数,seed为种子
        bytes_counter_(RandomPeriod()) {  // [0, 2MB-1]的服从均匀分布的随机值, 平均1MB,用来实现随机数，已实现读的优化(指导compaction)
  }
  virtual ~DBIter() {
    delete iter_;
  }
  virtual bool Valid() const { return valid_; }
  virtual Slice key() const {
    assert(valid_);
    return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
  }
  virtual Slice value() const {
    assert(valid_);
    return (direction_ == kForward) ? iter_->value() : saved_value_;
  }
  virtual Status status() const {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  virtual void Next();
  virtual void Prev();
  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();

 private:
  void FindNextUserEntry(bool skipping, std::string* skip);
  void FindPrevUserEntry();
  bool ParseKey(ParsedInternalKey* key);

  inline void SaveKey(const Slice& k, std::string* dst) {
    dst->assign(k.data(), k.size());
  }

  inline void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) { //即大于1MB,除了clear()还有回收内存空间的作用
      std::string empty;
      swap(empty, saved_value_);
    } else {
      saved_value_.clear(); //无回收空间的作用
    }
  }

  // Pick next gap with average value of config::kReadBytesPeriod.
  // 返回的是[0, 2MB-1]的服从均匀分布的随机值
  ssize_t RandomPeriod() {
    return rnd_.Uniform(2*config::kReadBytesPeriod);//caixin: static const int kReadBytesPeriod = 1048576; //为1024*1024
  }

  DBImpl* db_;
  const Comparator* const user_comparator_;
  Iterator* const iter_;
  SequenceNumber const sequence_;

  Status status_;
  std::string saved_key_;     // == current key when direction_==kReverse
  std::string saved_value_;   // == current raw value when direction_==kReverse
  Direction direction_;
  bool valid_;

  Random rnd_;
  ssize_t bytes_counter_;

  // No copying allowed
  DBIter(const DBIter&);
  void operator=(const DBIter&);
};

//对迭代器iter_->key()转换换成ParseInternalKey类型赋给ikey
//(迭代器平均遍历1MB的数据调用一次RecordReadSample以可以指导compaction,以实现读的优化作用,再调用MaybeScheduleCompaction,可能启动后台线程进行compaction)
inline bool DBIter::ParseKey(ParsedInternalKey* ikey) {
  Slice k = iter_->key(); //为iternalKey
  ssize_t n = k.size() + iter_->value().size();
  bytes_counter_ -= n; //初始值为 [0, 2MB-1]的服从均匀分布的随机值
  while (bytes_counter_ < 0) {
    bytes_counter_ += RandomPeriod();  // 加"服从[0, 2MB-1]的均匀分布的随机值"
    db_->RecordReadSample(k);      //平均1MB的数据调用一次,可以指导compaction，以实现读的优化作用,再调用MaybeScheduleCompaction,可能启动后台线程进行compaction)
  }
  if (!ParseInternalKey(k, ikey)) {
    status_ = Status::Corruption("corrupted internal key in DBIter");
    return false;
  } else {
    return true;
  }
}

void DBIter::Next() {
  assert(valid_);

  if (direction_ == kReverse) {  // Switch directions?
    direction_ = kForward;
    // iter_ is pointing just before the entries for this->key(),
    // so advance into the range of entries for this->key() and then
    // use the normal skipping code below.
    if (!iter_->Valid()) {
      iter_->SeekToFirst();
    } else {
      iter_->Next();
    }
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
    // saved_key_ already contains the key to skip past.
  } else { //设置saved_key_为当前的key
    // Store in saved_key_ the current key so we skip it below.
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
  }

//当skipping参数为true时,功能描述为：
//第一: seek是针对ikey.sequence<=sequence_的记录(即快照功能)
//第二：当前user key的所有记录隐藏,seek下一个userkey,直至遇到下一个userkey对应的第一条记录的类型为kTypeValue类型（skip被设置为最后一个被忽略的user key的值）,或遍历完整个数据库（此时,clear saved_key_, valid_为false）, 
//第三： 实现了读的优化，(迭代器平均遍历1MB的数据调用一次RecordReadSample以指导compaction,以实现读的优化作用,再调用MaybeScheduleCompaction,可能启动后台线程进行compaction)
  FindNextUserEntry(true, &saved_key_);
}

//两个参数的意思是：当skipping 为true时，则跳过userkey 为skip的记录，找到下一个满足条件的userkey
//                  当skipping 为false时，则直接从现在寻找满足条件的userkey
//当skipping参数为false时,功能描述为：
//第一: seek是针对ikey.sequence<=sequence_的记录(即快照功能)
//第二： 若关于某一user key的第一条记录是kTypeValue类型的，则不再seek（seek定位到该user key的第一条记录），skip不设置
//       若关于某一user key的第一条记录是kTypeDelete类型的，则该user key的所有记录隐藏,seek下一个userkey,直至遇到到上一个"若"中的情况（skip被设置为最后一个被忽略的user key的值）,或遍历完整个数据库（此时,clear saved_key_, valid_为false）,
//第三： 实现了读的优化，(迭代器平均遍历1MB的数据调用一次RecordReadSample以指导compaction,以实现读的优化作用,再调用MaybeScheduleCompaction,可能启动后台线程进行compaction)见ParseKey函数

//当skipping参数为true时，上面的第二点的第一条，变为：
//     当前skip(user key)的所有记录隐藏,seek下一个userkey,直至遇到下一个userkey对应的第一条记录的类型为kTypeValue类型（skip被设置为最后一个被忽略的user key的值）,或遍历完整个数据库（此时,clear saved_key_, valid_为false）,
void DBIter::FindNextUserEntry(bool skipping, std::string* skip) {
  // Loop until we hit an acceptable entry to yield
  assert(iter_->Valid());
  assert(direction_ == kForward);
  do {
    ParsedInternalKey ikey;
    if (ParseKey(&ikey) && ikey.sequence <= sequence_) {//注意只针对ikey.sequence <= sequence_的记录实现了快照功能
      switch (ikey.type) {
        case kTypeDeletion:
          // Arrange to skip all upcoming entries for this key since
          // they are hidden by this deletion.
          SaveKey(ikey.user_key, skip);//ikey.user_key赋给skip
          skipping = true;
          break;
        case kTypeValue:
          if (skipping && //初始时为false，可能被上面设置（若同一userkey的的第一条记录为"deletion"类型"实现该user key的所有记录隐藏）
              user_comparator_->Compare(ikey.user_key, *skip) <= 0) {
            // Entry hidden
          } else {
            valid_ = true;
            saved_key_.clear();
            return;     //返回
          }
          break;
      }
    }
    iter_->Next();
  } while (iter_->Valid());
  saved_key_.clear();
  valid_ = false; //不能再上面返回说明后面没有相应记录了
}

//功能，找到上一个有效的（不能被删除，sequence number< sequence_)的新的记录的值赋值给saved_key_, saved_value_细节如下：

//若一开始direction的方向为kForward则转换方向,将saved_key_设置为当前的key，将迭代器seek到上一个user key的所有记录的末端(最大值)
//倒着的方向遍历，目的是设置saved_key_, saved_value_, 需满足:saved_key_对应的记录的类型的为kTypeValue,且可以倒着找到一个user key < saved_key_的记录,或倒着遍历了整个数据库，那么这个saved_key_和saved_value_就是要的,此时valid_设置为true
//注意只针对ikey.sequence <= sequence_的记录实现了快照功能
//(迭代器平均遍历1MB的数据调用一次RecordReadSample以可以指导compaction,以实现读的优化作用,再调用MaybeScheduleCompaction,可能启动后台线程进行compaction),见ParseKey函数
void DBIter::Prev() {
  assert(valid_);

  if (direction_ == kForward) {  // Switch directions?
    // iter_ is pointing at the current entry.  Scan backwards until
    // the key changes so we can use the normal reverse scanning code.
    assert(iter_->Valid());  // Otherwise valid_ would have been false
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
    while (true) {
      iter_->Prev();
      if (!iter_->Valid()) {
        valid_ = false;
        saved_key_.clear();
        ClearSavedValue();
        return;
      }
      if (user_comparator_->Compare(ExtractUserKey(iter_->key()),//seek到上个user key的所有记录的末端(最大值),这是调用FindPrevUserEntry的要求
                                    saved_key_) < 0) {
        break;
      }
    }
    direction_ = kReverse;
  }
  //倒着的方向遍历，目的是设置saved_key_, saved_value_, 需满足:saved_key_对应的记录的类型的为kTypeValue,且可以倒着找到一个user key < saved_key_的记录,或倒着遍历了整个数据库，那么这个saved_key_和saved_value_就是要的,此时valid_设置为true
  //注意只针对ikey.sequence <= sequence_的记录实现了快照功能
  //(迭代器平均遍历1MB的数据调用一次RecordReadSample以可以指导compaction,以实现读的优化作用,再调用MaybeScheduleCompaction,可能启动后台线程进行compaction),见ParseKey函数
  FindPrevUserEntry();
}

//调用前需要迭代器已经seek到同一user key的所有记录的末端(最大值)
//注意只针对ikey.sequence <= sequence_的记录实现了快照功能
//倒着的方向遍历，目的是设置saved_key_, saved_value_, 需满足:saved_key_对应的记录的类型的为kTypeValue,且可以倒着找到一个user key < saved_key_的记录,或倒着遍历了整个数据库，那么这个saved_key_和saved_value_就是要的,此时valid_设置为true
//(迭代器平均遍历1MB的数据调用一次RecordReadSample以可以指导compaction,以实现读的优化作用,再调用MaybeScheduleCompaction,可能启动后台线程进行compaction),见ParseKey函数
void DBIter::FindPrevUserEntry() {
  assert(direction_ == kReverse);

  ValueType value_type = kTypeDeletion;
  if (iter_->Valid()) {
    do {
      ParsedInternalKey ikey;
      if (ParseKey(&ikey) && ikey.sequence <= sequence_) {//注意只针对ikey.sequence <= sequence_的记录实现了快照功能
        if ((value_type != kTypeDeletion) &&                            //saved_key_对应的记录类型为kTypeValue, 且
            user_comparator_->Compare(ikey.user_key, saved_key_) < 0) {//倒着找到一个user key < saved_key_的记录,那么这个saved_key_就是要的
          // We encountered a non-deleted value in entries for previous keys,
          break;
        } 
        //不可能一开始就执行下面，若刚开始调用本函数saved_key_为空的string，或者没有调用过Prev则只可能相等
        value_type = ikey.type;
        if (value_type == kTypeDeletion) {
          saved_key_.clear();
          ClearSavedValue();
        } else { //当找到kTypeValue类型,赋值saved_key_(user key), saved_value_
          Slice raw_value = iter_->value();
          if (saved_value_.capacity() > raw_value.size() + 1048576) {//若分配的空间过大，回收一部分内存空间
            std::string empty;
            swap(empty, saved_value_);
          }
          SaveKey(ExtractUserKey(iter_->key()), &saved_key_); //为saved_key_赋值
          saved_value_.assign(raw_value.data(), raw_value.size());
        }
      }
      iter_->Prev();
    } while (iter_->Valid());
  }

  if (value_type == kTypeDeletion) {
    // End
    valid_ = false;
    saved_key_.clear();
    ClearSavedValue();
    direction_ = kForward;
  } else {
    valid_ = true;
  }
}

void DBIter::Seek(const Slice& target) {
  direction_ = kForward;
  ClearSavedValue();
  saved_key_.clear();
  AppendInternalKey(
      &saved_key_, ParsedInternalKey(target, sequence_, kValueTypeForSeek));
  iter_->Seek(saved_key_);
  if (iter_->Valid()) {
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToFirst() {
  direction_ = kForward;
  ClearSavedValue();
  iter_->SeekToFirst();//一个综合的迭代器，找到最小的那个
  if (iter_->Valid()) {
    //功能描述为：
    //第一: seek是针对ikey.sequence<=sequence_的记录(即快照功能)
    //第二： 若关于某一user key的第一条记录是kTypeValue类型的，则不再seek（seek定位到该user key的第一条记录），skip不设置
    //       若关于某一user key的第一条记录是kTypeDelete类型的，则该user key的所有记录隐藏,seek下一个userkey,直至遇到到上一个"若"中的情况（skip被设置为最后一个被忽略的user key的值）,或遍历完整个数据库（此时,clear saved_key_, valid_为false）,
    //第三： 实现了读的优化，(迭代器平均遍历1MB的数据调用一次RecordReadSample以指导compaction,以实现读的优化作用,再调用MaybeScheduleCompaction,可能启动后台线程进行compaction)
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToLast() {
  direction_ = kReverse;
  ClearSavedValue();
  iter_->SeekToLast();//一个综合的迭代器，找到最大的那个
  //倒着的方向遍历，目的是设置saved_key_, saved_value_, 需满足:saved_key_对应的记录的类型的为kTypeValue,且可以倒着找到一个user key < saved_key_的记录,或倒着遍历了整个数据库，那么这个saved_key_和saved_value_就是要的,此时valid_设置为true
   //(迭代器平均遍历1MB的数据调用一次RecordReadSample以可以指导compaction,以实现读的优化作用,再调用MaybeScheduleCompaction,可能启动后台线程进行compaction),见ParseKey函数
  FindPrevUserEntry();
}

}  // anonymous namespace

Iterator* NewDBIterator(
    DBImpl* db,
    const Comparator* user_key_comparator,
    Iterator* internal_iter,
    SequenceNumber sequence, //>该sequence的记录不可见
    uint32_t seed) {
  return new DBIter(db, user_key_comparator, internal_iter, sequence, seed);
}

}  // namespace branchdb
