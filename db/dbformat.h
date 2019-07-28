// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_BRANCHDB_DB_FORMAT_H_
#define STORAGE_BRANCHDB_DB_FORMAT_H_

#include <stdio.h>
#include "branchdb/comparator.h"
#include "branchdb/db.h"
#include "branchdb/filter_policy.h"
#include "branchdb/slice.h"
#include "branchdb/mtable_builder.h"
#include "util/coding.h"
#include "util/logging.h"

namespace branchdb {

// Grouping of constants.  We may want to make some of these
// parameters set via options.
namespace config {
static const int kNumLevels = 7;

//const uint64_t fileCommonSize = 4<<20;
//const uint64_t fileCommonSize = 32<<20; //4MB->32MB
const uint64_t fileCommonSize = 128<<20; //4MB->128MB(平均64MB)

#ifndef NDEBUG
static const int levelTimes = 4;   //最小值应该为2
#else
static const int levelTimes = 10;
#endif

static const int splitTriggerNum = levelTimes << 1;

static const int instableRangeSplitNum = (levelTimes+1)/2;
static const int instableRangeInitialDataSize = fileCommonSize / instableRangeSplitNum; //此处不应该修改

// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
static const int kMaxMemShiftLevel = 2;

static const int kfullUsePercent = 95; //为了使一般格式更多，这里设置为95%就好了

// Approximate gap in bytes between samples of data read during iteration.
static const int kReadBytesPeriod = 1048576; //为1024*1024

static const int backThreadNum = 4;

static const int mmapReadFileNum = 0; //默认关闭mmap函数读取文件(leveldb原来是1000,一个文件2MB,故为2G),对于内存小于存储的数据时,最好关闭本选项（总结于RocksDB)

static const int rgNumThresholdForMerge = levelTimes; //可能触发"结合归并策略"的树所存储的最小的range数,同时要触发需满足抽样的个数要大于本值,用于VersionSet::setDataInfo函数中

static const int samplingRgNum = levelTimes;          //每次抽样的range数,用于VersionSet::setDataInfo函数中

static const int leastAppendLevel = 0;          //对该层以及该层上面的层只进行进行append操作(不结合归并策略)的最小层的索引(若不设置append层,则设为-1), 用于SetIsChildMerge函数中

static const int yellow_sleep_us= 500;  //对于SSD推荐200s，对于HDD推荐1000s

static const bool affordLongAdjustTimeAfterLoad = false; //只对后台线程为单线程时有效,true表示有效，用于控制在load完后，数据量很大控制后台调整的时间(使其不超过半小时)。


}  // namespace config

class InternalKey;

// Value types encoded as the last component of internal keys.
// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
// data structures.
enum ValueType {
  kTypeDeletion = 0x0,
  kTypeValue = 0x1
};
// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
static const ValueType kValueTypeForSeek = kTypeValue;

typedef uint64_t SequenceNumber;

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
static const SequenceNumber kMaxSequenceNumber =
    ((0x1ull << 56) - 1);//最多占用56位（剩余8位0)

struct ParsedInternalKey {
  Slice user_key;
  SequenceNumber sequence;
  ValueType type;

  ParsedInternalKey() { }  // Intentionally left uninitialized (for speed)
  ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
      : user_key(u), sequence(seq), type(t) { }
  std::string DebugString() const;
};

// Return the length of the encoding of "key".
inline size_t InternalKeyEncodingLength(const ParsedInternalKey& key) {
  return key.user_key.size() + 8;
}

// Append the serialization of "key" to *result.
extern void AppendInternalKey(std::string* result,
                              const ParsedInternalKey& key);

// Attempt to parse an internal key from "internal_key".  On success,
// stores the parsed data in "*result", and returns true.
//
// On error, returns false, leaves "*result" in an undefined state.
extern bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result);

// Returns the user key portion of an internal key.
// 返回internal_key的 user key
inline Slice ExtractUserKey(const Slice& internal_key) {
  assert(internal_key.size() >= 8);
  return Slice(internal_key.data(), internal_key.size() - 8);
}

inline ValueType ExtractValueType(const Slice& internal_key) {
  assert(internal_key.size() >= 8);
  const size_t n = internal_key.size();
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  unsigned char c = num & 0xff;
  return static_cast<ValueType>(c);
}

// A comparator for internal keys that uses a specified comparator for
// the user key portion and breaks ties by decreasing sequence number.
// Comparator为一个抽象基类
class InternalKeyComparator : public Comparator {//线程安全的类(这是要求实现Comparator接口所要求的
 private:                            
  const Comparator* user_comparator_;//抽象基类的const 的指针, 用于指向用户的定义的实现Comparator接口的类
 public:                             //memtable 中user_comparator_的指针为 BytewiseComparatorImpl 
  explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c) { } //memtable中BytewiseComparatorImpl 类型的指针调用
  virtual const char* Name() const;
  virtual int Compare(const Slice& a, const Slice& b) const;//将a,b分成了user key 和尾部两部分比较, dbformat.cc中定义
  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const;
  virtual void FindShortSuccessor(std::string* key) const;
  //寻找a,b中间的string, 对于InternalKey没有调用场景，所以不应调用
  virtual std::string MiddleString(const Slice& a, const Slice& b, bool plusOne=false) const{
    assert(0);
    return std::string("");
  }

  const Comparator* user_comparator() const { return user_comparator_; }//返回私有成员变量

  int Compare(const InternalKey& a, const InternalKey& b) const;//调用了上面的重载Compare 方法
};

// Filter policy wrapper that converts from internal keys to user keys
class InternalFilterPolicy : public FilterPolicy {
 private:
  const FilterPolicy* const user_policy_;
 public:
  explicit InternalFilterPolicy(const FilterPolicy* p) : user_policy_(p) { }
  virtual const char* Name() const;
  virtual void CreateFilter(const Slice* keys, int n, std::string* dst) const;
  virtual bool KeyMayMatch(const Slice& key, const Slice& filter) const;
};

// Modules in this directory should keep internal keys wrapped inside
// the following class instead of plain strings so that we do not
// incorrectly use string comparisons instead of an InternalKeyComparator.
class InternalKey {
 private:
  std::string rep_;// 格式为：user key + tag(sequence number高7字节 + type 1字节)共8位
  friend inline bool isEqual(InternalKey& a, InternalKey& b);
 public:
  InternalKey() { }   // Leave rep_ as empty to indicate it is invalid
  InternalKey(const Slice& user_key, SequenceNumber s, ValueType t) {
    AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t));
  }
 
  bool isEmpty() const { return rep_.empty(); }
  void DecodeFrom(const Slice& s) { rep_.assign(s.data(), s.size()); }
  Slice Encode() const {
    assert(!rep_.empty());
    return rep_;//用了隐式类型转换 string --->  Slice
  }

  Slice user_key() const { return ExtractUserKey(rep_); }

  void SetFrom(const ParsedInternalKey& p) {
    rep_.clear();
    AppendInternalKey(&rep_, p);
  }

  void Clear() { rep_.clear(); }

  const std::string& getString() const { return rep_; }

  std::string DebugString() const;
};

inline bool isEqual(InternalKey& a, InternalKey& b) {
  return  a.rep_.compare(b.rep_) == 0;
}

inline int InternalKeyComparator::Compare(
    const InternalKey& a, const InternalKey& b) const {
  return Compare(a.Encode(), b.Encode());
}

//用于version_edit/version_set .h/cc中构建delete set时使用, 其实根据何种排序规则排序不重要只是为了查找迅速而已
inline bool operator< (const InternalKey& a, const InternalKey& b){
  InternalKeyComparator icmp(BytewiseComparator());
  return icmp.Compare(a,b) < 0;
}


//将internal_key解析至*result中
//kTypeValue字段出错，返回false(因为其他字段可以为任意值)
inline bool ParseInternalKey(const Slice& internal_key,
                             ParsedInternalKey* result) {
  const size_t n = internal_key.size();
  if (n < 8) return false;
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  unsigned char c = num & 0xff;
  result->sequence = num >> 8;
  result->type = static_cast<ValueType>(c);
  result->user_key = Slice(internal_key.data(), n - 8);
  return (c <= static_cast<unsigned char>(kTypeValue));
}

// A helper class useful for DBImpl::Get()
class LookupKey {
 public:
  // Initialize *this for looking up user_key at a snapshot with
  // the specified sequence number.
  LookupKey(const Slice& user_key, SequenceNumber sequence);//将传入的参数构造成下面private成员描述的格式

  ~LookupKey();

  // Return a key suitable for lookup in a MemTable.
  Slice memtable_key() const { return Slice(start_, end_ - start_); }

  // Return an internal key (suitable for passing to an internal iterator)
  Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }

  // Return the user key
  Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }

 private:
  // We construct a char array of the form:
  //    klength  varint32               <-- start_   //userkey.size() + 8
  //    userkey  char[klength]          <-- kstart_
  //    tag      uint64
  //                                    <-- end_
  // The array is a suitable MemTable key.
  // The suffix starting with "userkey" can be used as an InternalKey.
  const char* start_;
  const char* kstart_;
  const char* end_;
  char space_[200];      // Avoid allocation for short keys, 即小于200-8-5 = 187的字节不分配动态空间

  // No copying allowed
  LookupKey(const LookupKey&);
  void operator=(const LookupKey&);
};

inline LookupKey::~LookupKey() {
  if (start_ != space_) delete[] start_;
}


}  // namespace branchdb

#endif  // STORAGE_BRANCHDB_DB_FORMAT_H_
