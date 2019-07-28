// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <stdlib.h>
#include <algorithm>
#include <stdio.h>
#include <math.h>
#include <stdint.h>
#include <set>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/mtable_cache.h"
#include "db/dbformat.h"
#include "branchdb/env.h"
#include "branchdb/options.h"
#include "branchdb/mtable_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/file_in_RAM.h"

namespace branchdb {

//ok
static int64_t  StandardTableNumForLevel(int level) {
  uint64_t result = config::levelTimes;
  while(level >=1) {
    result *= config::levelTimes;
    --level;
  }
  return result;
}
//ok
static int64_t  MaxTableNumForLevel(int level) {
  int64_t standardNum = StandardTableNumForLevel(level);
  int64_t result = standardNum + config::levelTimes <= ceil((1+1.0/config::levelTimes)*standardNum) ? standardNum + config::levelTimes : ceil((1+1.0/config::levelTimes)*standardNum);
  return result;
}

static int64_t MaxDiff(int level) {
  return MaxTableNumForLevel(level) - StandardTableNumForLevel(level);
}

//只用于 flush到非稳定层(overlapRanges.size()!=0) 和 非稳定层的某个range进行垃圾回收(overlapRanges.size()==0) 时使用
//对于imm或range下移到非稳定层，其在非稳定层相交的range数组为overlapRanges，返回其下移最多可能"纯增加"的range的个数(最多新生成的-删除的), 通过参数maxNewedNumForInsable返回其最多能新建的range数
static int getAdding_NewNumForInstable(MemTable* imm, RangeMetaData* range, std::vector<RangeMetaData*> &overlapRanges, int& maxNewedNumForInsable) {
  assert( !range && imm || range && !imm ); //有且仅有一个为非NULL
  bool lastRangeIsFull = false;
  int lastTotalData = 0;
  int downDataSize = ( imm == NULL ? range->DataMount() : imm->ApproximateMemoryUsage() ); //可能不准确,但是没关系,因为实际操作时会控制增加的个数<=最大值
  int maxNewNum = 0;
  int fullNum = 0;
  for(int i = 0 ; i < overlapRanges.size(); ++i) {
    if( overlapRanges[i]->isFull() ) {
      ++fullNum;
      if(!lastRangeIsFull) {
        lastRangeIsFull = true;
        lastTotalData = overlapRanges[i]->DataMount();
      }
      else
        lastTotalData += overlapRanges[i]->DataMount();
    }
    else {
      if(lastRangeIsFull) maxNewNum  += ( (downDataSize + lastTotalData)/config::instableRangeInitialDataSize + \
                                          ( (downDataSize + lastTotalData)%config::instableRangeInitialDataSize ? 1 : 0 ) );
      lastRangeIsFull = false;
      lastTotalData = 0;
    }
  }
  if(overlapRanges.size() == 0 || overlapRanges[overlapRanges.size()-1]->isFull() )
       maxNewNum  += ( (downDataSize + lastTotalData)/config::instableRangeInitialDataSize + \
                      ( (downDataSize + lastTotalData)%config::instableRangeInitialDataSize ? 1 : 0 ) );
  maxNewedNumForInsable = maxNewNum;
  if(overlapRanges.size() == 0) { //为非稳定层的range的垃圾回收
    return maxNewNum - 1; //减去自己(将会被删除的)
  } else {                         //flush到非稳定层的情况
    return maxNewNum - fullNum; //减去原来满的(将会被删除的)
  }
}


bool VersionSet::ShouldReduceRange(int level) {
  assert(level >= -1);
  if (level == -1) return true; 
  return WillLeastNumRange(level) > StandardTableNumForLevel(level);
}
//对于非稳定层，在添加之前需要满足其range个数小于阈值;对于稳定层，在添加之前需要满足其range个数小于最大值
bool VersionSet::CanAddRange(int level) {
  assert(level >= 0);
  if(level == maxStableLevelIndex_ + 1)             //对于非稳定层，在添加之前需要满足其个数小于阈值
    return WillMostNumRange(level) < StandardTableNumForLevel(level);
  else                                                   //对于稳定层，在添加之前需要满足其个数小于最大值
    return WillMostNumRange(level) < MaxTableNumForLevel(level);
}

//对于非稳定层，在添加后需要满足的range个数小于等于阈值;对于稳定层，在添加之后需要满足其range个数小于等于最大值
int VersionSet::CanAddMostRangeNum(int level) {
  assert(level >=0);
  if(level == maxStableLevelIndex_ + 1)             //对于非稳定层，在添加之前需要满足其个数小于阈值
    return StandardTableNumForLevel(level) - WillMostNumRange(level);
  else                                                   //对于稳定层，在添加之前需要满足其个数小于最大值
    return MaxTableNumForLevel(level) - WillMostNumRange(level);
}

//ok
static int64_t TotalFileSize(const std::vector<RangeMetaData*>& ranges, bool includeMeta = true) {
  int64_t sum = 0;
  for (std::vector<RangeMetaData*>::const_iterator iter = ranges.begin(); iter != ranges.end(); ++iter) {
    sum += (*iter)->DataMount(includeMeta);
  }
  return sum;
}

//Version 的析构函数(私有)
//ok
Version::~Version() {
  assert(refs_ == 0);//该version的引用为0才析构

  // Remove from linked list(从双向链表中删除该节点)
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files(该vertion的所有level的所有RangeMetaData的引用减一，若引用为0删除该RangeMetaData)
  for (int level = 0; level < config::kNumLevels; level++) {
    std::vector<RangeMetaData*>& ranges = ranges_[level];
    for (std::vector<RangeMetaData*>::iterator iter = ranges.begin(); iter != ranges.end(); ++iter) {
      (*iter)->Unref();
    }
  }
}

// Return the smallest index i such that ranges[i]->largest >= key.
// 若isByUserkey为false则基于internalKey比较; 若为true,则基于user key比较,对于查找覆盖一般基于user key，读取时一般基于InternalKey
// 若不存在，则返回ranges.size(),若ranges为空,返回的为0
// 在ranges使用二分查找法, key为internalKey还是userkey基于参数isByUserkey的序列化之后的
// ok
int FindRange(const InternalKeyComparator& icmp,
             const std::vector<RangeMetaData*>& ranges,
             const Slice& key,
             bool isByUserkey ) { //该参数默认为false
  uint32_t left = 0;
  uint32_t right = ranges.size();
  while (left < right) { //二分查找的变体
    uint32_t mid = (left + right) / 2;
    const RangeMetaData* r = ranges[mid];
    if (!isByUserkey && icmp.InternalKeyComparator::Compare(r->largest.Encode(), key) < 0  || \
         isByUserkey && icmp.user_comparator()->Compare(r->largest.user_key(), key) < 0 ) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}


//ok
static bool AfterRange(const Comparator* ucmp,
                      const Slice* user_key, const RangeMetaData* r) {
  // NULL user_key occurs before all keys and is therefore never after *r
  return (user_key != NULL &&
          ucmp->Compare(*user_key, r->largest.user_key()) > 0);
}

//ok
static bool BeforeRange(const Comparator* ucmp,
                       const Slice* user_key, const RangeMetaData* r) {
  // NULL user_key occurs after all keys and is therefore never before *r
  return (user_key != NULL &&
          ucmp->Compare(*user_key, r->smallest.user_key()) < 0);
}

//判断ranges中是否存在与[smallest_user_key,largest_user_key](不是InternalKey)有overlap, 若有:返回 true; 若ranges为空,返回的为false
//用二分法搜索该ranges
//ok
extern bool SomeRangeOverlapsRange(
    const InternalKeyComparator& icmp,
    const std::vector<RangeMetaData*>& ranges,
    const Slice* smallest_user_key,
    const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();//默认返回为 BytewiseComparatorImpl *

  // Binary search over range list(FindRange中)
  // 首先找到第一个 index i such that ranges[i]->largest >=  small.
  uint32_t index = 0;
  if (smallest_user_key != NULL) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small(*smallest_user_key, kMaxSequenceNumber,kValueTypeForSeek);
    index = FindRange(icmp, ranges, small.Encode());//若index有效，为overlap的候选者,还需要下面BeforeFile进一步判定
  }

  if (index >= ranges.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }
  
  //然后需满足largest_user_key要 >= range[index].smallest.user_key()
  return !BeforeRange(ucmp, largest_user_key, ranges[index]);
}


// Store task in "*overlapTasks" in tasks that overlap [begin,end],注意函数内部实际比较的是user key
// ok
void VersionSet::GetOverlappingTasks(
    const InternalKeyComparator& icmp,
    const std::set<FullTask, BySmallest>& tasks, //用set的方式存储，不适合用二分查找方式
    const InternalKey* begin,
    const InternalKey* end,
    std::vector<FullTask>* overlapTasks) {
  assert(begin != NULL && end != NULL); 
  overlapTasks->clear();
  Slice user_begin, user_end;
  user_begin = begin->user_key();
  user_end = end->user_key();
  const Comparator* user_cmp = icmp.user_comparator();//默认为BytewiseComparatorImpl *

  for (std::set<FullTask>::iterator iter = tasks.begin(); iter != tasks.end(); ++iter ) {
    RangeMetaData* r = iter->rg;
    const Slice file_start = r->smallest.user_key();
    const Slice file_limit = r->largest.user_key();
    if (user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
    } else if (user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
      break; //因为task是排好序的，后面的不满足的
    } else { //有重叠区域
      overlapTasks->push_back(*iter);
    }
  }
}


// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
//在某个version的某一个level(或某一序列的,根据InternalKey排好序)的文件list, 根据key，定位到哪个文件中包含。返回的key为该文件的最大internalkey的编码，value为file number + index_offset_end + index_offset + appendTimes共32字节
//注意这个list的需满足，REQUIRES: "files" contains a sorted list of non-overlapping files.
//ok
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<RangeMetaData*>* rlist) //某一层的,或一个list(VersionSet::MakeInputIterator使用)
      : icmp_(icmp),
        rlist_(rlist),
        index_(rlist->size()) {        // Marks as invalid(因为从0开始)
  }
  virtual bool Valid() const {
    return index_ < rlist_->size();
  }
  //定位到第一个 (*rlist_)[i]->largest>= target
  virtual void Seek(const Slice& target) {
    index_ = FindRange(icmp_, *rlist_, target);// Return the smallest index i such that (*rlist_)[i]->largest >= key. Return ranges.size() if there is no such ranges. REQUIRES: "ranges" contains a sorted list of non-overlapping files.
    nextFirstNoFileRange();
  }
  virtual void SeekToFirst() {
    index_ = 0;
    nextFirstNoFileRange();
  }
  virtual void SeekToLast() {
    index_ = rlist_->empty() ? 0 : rlist_->size() - 1;
    prevFirstNoFileRange();
  }
  virtual void Next() {
    assert(Valid());
    index_++;
    nextFirstNoFileRange();
  }
  virtual void Prev() {
    assert(Valid());
    if (index_ == 0) {
      index_ = rlist_->size();  // Marks as invalid
    } else {
      index_--;
    }
    prevFirstNoFileRange();
  }
  Slice key() const {
    assert(Valid());
    return (*rlist_)[index_]->largest.Encode();
  }
  //16-byte value containing the file number and file size, both
  Slice value() const {
    assert(Valid());
    EncodeFixed64(value_buf_, (*rlist_)[index_]->filemeta->file_number);
    EncodeFixed64(value_buf_+8, (*rlist_)[index_]->index_offset_end);
    EncodeFixed64(value_buf_+16, (*rlist_)[index_]->index_offset);
    EncodeFixed64(value_buf_+24, (*rlist_)[index_]->appendTimes);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  virtual Status status() const { return Status::OK(); }
 private:

  //新增以下两个函数，以跳过有range（必定分配了file number）,但没有磁盘文件的情况
  void nextFirstNoFileRange() {
    while( Valid() && (*rlist_)[index_]->usePercent == 0 ) {
      assert((*rlist_)[index_]->appendTimes == 0); //与上述条件等价，因为即使被append的没有数据(其实也不会发生), 也有其他信息使得usePercent非0(向上取整)
      index_++;
    }
  }
  void prevFirstNoFileRange() {
    while( Valid() && (*rlist_)[index_]->usePercent == 0 ) {
      assert((*rlist_)[index_]->appendTimes == 0); //与上述条件等价，因为即使被append的没有数据(其实也不会发生), 也有其他信息使得usePercent非0(向上取整)
      if (index_ == 0) {
        index_ = rlist_->size();  // Marks as invalid
      } else {
        index_--;
      }
    }
  }

  const InternalKeyComparator icmp_;
  const std::vector<RangeMetaData*>* const rlist_;
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[32];
};

//file_value有file number, index_offset_end 和index_offset组成，
//该函数利用file_value获得Table_cache的内部迭代器（若cache中没有则构造后再返回）
//ok
static Iterator* GetFileIterator(void* arg,
                                 const ReadOptions& options,
                                 const Slice& file_value) {
  MtableCache* cache = reinterpret_cast<MtableCache*>(arg);
  if (file_value.size() != 32) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    uint64_t file_number = DecodeFixed64(file_value.data());
    if( file_number > 0 ) {
      return cache->NewIterator(options,
                        file_number,
                        DecodeFixed64(file_value.data() + 8), //index_offset_end
                        DecodeFixed64(file_value.data() + 16),//index_offset
                        DecodeFixed64(file_value.data() + 24));//appendTimes
    }
    else {
      assert(file_number == 0);
      return NewEmptyIterator();
    }
  }
}

//获得level层的迭代器
//ok
Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &ranges_[level]), //第一层迭代器，生成可得到file number,index_offset_end,index_offset(根据key，找到哪个文件中包含)
      &GetFileIterator, vset_->mtable_cache_, options);//生成第二层迭代器(利用file_value获得Table_Cache的内部迭代器)函数,和函数中的两个参数
}

//向(*iters)中加入某一个version中的所有迭代器(多个level), 迭代器的数量为：每个不空的level一个）
//ok
void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // For all levels , we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.(即迭代器，不满足不用打开可以不打开,但是打开过的文件根据LRU cache策略缓存在Mtable Cache和data block cache)
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!ranges_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));//push back 生成的某一层（level必须>0）的迭代器
    }
  }
}

// Callback from TableCache::Get()
// ok
namespace {
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};
struct Saver {
  SaverState state;  //输出
  const Comparator* ucmp;//SaveValue函数中作为输入
  Slice user_key; //SaveValue函数中作为输入
  std::string* value; //输出
};
}
//该函数将传入的参数（seek到的）ikey/value(在TableCache中,ikey为internal key) 的ikey中的user key与arg（Saver 类型）
//中的user key比较，若相等且ikey中的type为KTypevalue,则设置arg中的value值，arg中的state设为kFound，若type不是KTypevalue,
//则arg中的state设置为kDeleted
//ok
static bool SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
    return true; //表示有坏块,可以不继续寻找了
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {//若两者的user key相等
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());//设置value值
      }
      return true; //表示找到了相等的了就可以不继续寻找了
    }
  }
  return false; //不是相等的，故需要继续寻找
}

//函数RecordReadSample函数调用,该函数只需要找overlap了指定的user key文件 
//从上层到下层, 若通过internal_key 和Find File 找到的文件包含user key, 找到即调用(*func)(agr, level, fileMetaData*);
//若(*func)(agr, level, fileMetaData*)返回false立即退出,若该func为State::Match,当已经是第二次调用时即返回false,第一次调用时记录下range信息
void Version::ForEachOverlapping(Slice user_key, Slice internal_key,
                                 void* arg,
                                 bool (*func)(void*, int, RangeMetaData*)) {
  // TODO(sanjay): Change Version::Get() to use this function.(Get没有使用本函数)
  const Comparator* ucmp = vset_->icmp_.user_comparator();

  // Search other levels.
  for (int level = 0; level < config::kNumLevels; level++) {
    size_t num_ranges = ranges_[level].size();
    if (num_ranges == 0) continue;

    // Binary search to find earliest index whose largest key >= internal_key.
    uint32_t index = FindRange(vset_->icmp_, ranges_[level], internal_key);
    if (index < num_ranges) {
      RangeMetaData* r = ranges_[level][index];
      if (ucmp->Compare(user_key, r->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key
      } else { //某一level(非零)找到一个overlap了该user key文件, 用func处理后，即返回
        if (!(*func)(arg, level, r)) {//若该func为State::Match，当已经是第二次调用时即返回false
          return;
        }
      }
    }
  }
}




// Lookup the value for key.  If found, store it in *val and
// return OK.  Else return a non-OK status.  Fills *stats.
//注意stats填充：当读某个user key, 读了超过一次文件时设置该stats,记录了寻找的第一个文件的信息，函数未读memtable(因为非Verion管理)
// REQUIRES: lock is not held(因为访问成员变量file_，对于特定版本，不会再改变，而利用了TableCache的Get函数，而TableCache函数是线程安全的，故不需要持有mutex_
// ok
Status Version::Get(const ReadOptions& options,
                    const LookupKey& k,
                    std::string* value,
                    GetStats* stats) {
  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();
  const Comparator* ucmp = vset_->icmp_.user_comparator();//默认为BytewiseComparatorImpl *
  Status s;

  stats->seek_range= NULL;
  stats->seek_range_level = -1;
  RangeMetaData* last_range_read = NULL;
  int last_range_read_level = -1;

  // We can search level-by-level since entries never hop(跳过) across
  // levels.  Therefore we are guaranteed that if we find data
  // in an smaller level, later levels are irrelevant.
  // 逐层搜索
  RangeMetaData* tmp;
  for (int level = 0; level < config::kNumLevels; level++) {
    size_t num_ranges = ranges_[level].size();
    if (num_ranges == 0) continue;

    // Get the list of ranges to search in this level
    RangeMetaData* const* ranges = &ranges_[level][0];//ranges指向一个const指针
    RangeMetaData* searchRange = NULL;
    // Binary search to find earliest index whose largest key >= ikey.
    uint32_t index = FindRange(vset_->icmp_, ranges_[level], ikey);

    // 若此时没有数据就没有文件(或appendTimes为0),无需继续搜索(否则会发生文件不存在的IO错误)
    if (index >= num_ranges || ranges_[level][index]->usePercent == 0) {
      searchRange= NULL;
    } else {
      tmp = ranges[index];
      if (ucmp->Compare(user_key, tmp->smallest.user_key()) < 0) {
        // All of "tmp" is past any data for user_key
        searchRange= NULL;
      } else {
        searchRange = tmp;
      }
    }

    //对searchRange进行搜索
    if(searchRange != NULL) { 
      if (searchRange->appendTimes > 0 && last_range_read != NULL && stats->seek_range== NULL) { //当且仅当第二次获得满足该if语句
        // We have had more than one seek for this read.  Charge the 1st file.
        //若需要的读的文件超过一次(包括level0)，则记录第一个读的文件的信息
        stats->seek_range = last_range_read;//注意循环条件，设置了一次后，不再设置
        stats->seek_range_level = last_range_read_level;
      }
     
      if(searchRange->appendTimes > 0) { 
        last_range_read = searchRange;
        last_range_read_level = level;
      }
      
      Saver saver;//用来保存Get后的状态
      saver.state = kNotFound;
      saver.ucmp = ucmp; //默认为BytewiseComparatorImpl *
      saver.user_key = user_key;
      saver.value = value; //注意为string*类型，设置了saver.value,即设置了返回值
      //设置saver.state和saver.value
      s = vset_->mtable_cache_->Get(options, searchRange->filemeta->file_number, searchRange->index_offset_end, searchRange->index_offset,
                                   searchRange->appendTimes, ikey, &saver, SaveValue);//saver作为SaveValue的第一个参数,其中的ikey为internalkey

      if (!s.ok()) {
        return s;
      }
      switch (saver.state) {
        case kNotFound:
          break;      // Keep searching in other files(搜索该level的下一个文件)
        case kFound: //成功
          return s;
        case kDeleted:
          s = Status::NotFound(Slice());  // Use empty error message for speed
          return s;
        case kCorrupt:
          s = Status::Corruption("corrupted key for ", user_key);
          return s;
      }
    }
    //搜索下一层
  }

  //所有level搜索完毕，没有找到
  return Status::NotFound(Slice());  // Use an empty error message for speed
}

// 唯一修改range_to_compact_和range_to_compact_level_的函数：
//       若stats记录的文件遇到的次数已经到达了一个阈值则根据该stats设置file_to_compact_ file_to_compact_level_两个成员变量并且返回true,否则返回false
//       用来进行读的优化
bool Version::UpdateStats(const GetStats& stats) {
  RangeMetaData* r = stats.seek_range;
  if (r != NULL) { //上一次Get函数读取了超过一个文件
    r->allowed_seeks--; //allowed_seeks与每个RangeMetaData文件绑定，默认初始时在RangeMetaData构造函数中初始化
    if (r->allowed_seeks <= 0 ) { //若满足，说明该sstable文件的效率底下(并且当前没有需要compct的文件）,设置两个成员变量指导compact
      for(int i = 0; i < vset_->options_->numMaxRangesForRO; ++i) {
        if (range_to_compact_[i] == NULL) {
          assert(range_to_compact_level_[i] == -1); 
          range_to_compact_[i] = r;
          range_to_compact_level_[i] = stats.seek_range_level;
        }
      }
      return true;//设置了成员变量
    }
  }
  return false; //未设置成员变量
}

//函数过程：根据key(internalkey),若overlap了该user key的文件大于等于两个(按照与Get函数一样的顺序寻找), 设置该第一个文件的Getstats类型对象,再调用UpdataStats(见>上面描述),可能设置range_to_compact_ range_to_compact_level_两个成员,如设置了,则返回true
//由DBIter类（DBImpl中实现的迭代器)调用，平均每解析1MB数据调用一次，以实现读的优化
bool Version::RecordReadSample(Slice internal_key) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    //第一次调用, 返回true； >=两次调用返回false。记录第一次调用的信息
    static bool Match(void* arg, int level, RangeMetaData* r) {
      State* state = reinterpret_cast<State*>(arg);
      if(r->appendTimes > 0) {
        state->matches++;
      }
      if (state->matches == 1) {
        // Remember first match.
        state->stats.seek_range = r;
        state->stats.seek_range_level = level;
      }
      // We can stop iterating once we have a second match.
      return r->appendTimes > 0 && state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);//该函数只需要找overlap了指定的user key文件(按照与Get函数一样的顺序寻找),找到一次调用>一次Match(函数使超过两次）

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

//ok
void Version::Ref() {
  ++refs_;
}

//版本的引用计数减一 ,若变为0:
//析构该version(从双向链表中删除该节点等,以及该version的所有level的所有RangeMetaData的引用减一，若引用为0删除该RangeMetaData)）
//ok
void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

//若指定level是否存在与[smallest_user_key,largest_user_key](不是InternalKey)有overlap, 若有:返回 true
//ok
bool Version::OverlapInLevel(int level,
                             const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeRangeOverlapsRange(vset_->icmp_, ranges_[level],
                               smallest_user_key, largest_user_key);
}

// Return the level at which we should place a new memtable compaction
// result that covers the range [smallest_user_key,largest_user_key].
//while选择某一level: 该level < kMaxMemShiftLevel且其range数大于等于标准值, 该level没有与其相交的且level+1也没有且level+2没有重叠不超过10, 则level++(即选择level+1)
//ok
int Version::PickLevelForMemTableOutput(
    const Slice& smallest_user_key,
    const Slice& largest_user_key) {
  int level = 0;
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {//level 0不存在与该范围有overlap的文件
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);//注意用kValueTypeForSeek(即0x1)的原因,因为type是大的反而小
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));//注意用0x0(ValueType)的原因,因为type是小的反而大
    std::vector<RangeMetaData*> overlaps;
    while (level < config::kMaxMemShiftLevel && ranges_[level].size() >= StandardTableNumForLevel(level)) { //kMaxMemShiftLevel 为2, 见dbformat.cc的解释
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        break;
      }
      if (level + 2 < config::kNumLevels) {
        // Check that file does not overlap too many grandparent bytes.
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        if (overlaps.size() > config::levelTimes) {
          break;
        }
      }
      level++;//level 没有，level+1没有， level+2没有重叠不超过10, 则level++
    }
  }
  return level;
}

// Store in "*inputs" all files in "level"(指定的level) that overlap [begin,end],注意函数内部实际比较的是user key
// 将level层中overlap [user begin,user end]的文件的闭包写入inputs(可能多个); 若twoEndIdx不为NULL，将孩子的最小和最大索引存入，若不存在孩子则为(i，i),i为the smallest index i such that ranges[i]->largest >= begin->user_key()
// ok
void Version::GetOverlappingInputs(
    int level,
    const InternalKey* begin,
    const InternalKey* end,
    std::vector<RangeMetaData*>* inputs,
    std::pair<int,int>* twoEndIdx) { //默认为NULL
  assert(level >= 0 && level < config::kNumLevels);
  assert(begin != NULL && end != NULL && inputs!=NULL); 
  inputs->clear();
  Slice user_begin, user_end;
  user_begin = begin->user_key();
  user_end = end->user_key();
  const Comparator* user_cmp = vset_->icmp_.user_comparator();//默认为BytewiseComparatorImpl *

  int i = FindRange(vset_->icmp_, ranges_[level], begin->user_key(), true); //基于user key, Return the smallest index i such that ranges[i]->largest >= begin.
  int num=0, firstIdx = i, endIdx = i;
  for (; i < ranges_[level].size(); i++) {
    RangeMetaData* r = ranges_[level][i];
    const Slice file_start = r->smallest.user_key();
    const Slice file_limit = r->largest.user_key();
    if (user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
      break;
    } else { //有重叠区域
      if(++num == 1) firstIdx = i;
      endIdx = i;
      inputs->push_back(r);
    }
  }
  if(twoEndIdx != NULL) *twoEndIdx = std::make_pair(firstIdx, endIdx);
}

// Return a human readable string that describes this version's contents(全部的level的全部文件的描述信息).
//格式为：file number:file size:actual file size[ smallest InternalKey .. largest internalKey]
//ok
std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 0 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<RangeMetaData*>& ranges = ranges_[level];
    for (size_t i = 0; i < ranges.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, ranges[i]->filemeta->file_number);
      r.push_back(':');
      AppendNumberTo(&r, ranges[i]->file_size()); //文件的长度(包括文件内部的洞)
      r.push_back(':');
      AppendNumberTo(&r, ranges[i]->usePercent); //文件的实际长度(不包括文件内部的洞)
      r.append("[");
      r.append(ranges[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(ranges[i]->largest.DebugString());
//      r.append(":  index_offset:");
//      AppendNumberTo(&r, ranges[i]->index_offset); //文件的实际长度(不包括文件内部的洞)
      r.append("]\n");
    }
  }
  return r;
}

//调用场景：1.在恢复数据库时从MANIFEST文件中恢复; 2.在某一任务完成磁盘操作时更新树的内存中的元信息使用
// 一次apply就进行一次全面的merge操作(可以优化，可并行执行的多个apply进行一次merge),不好做到可以像levelDB一样不管多少apply调用不产生中间version只进行一次全面的merge操操作,主要是因为modify range要利用之前的生成好的版本信息
// 调用的格式为 Builder + Apply* + SaveTo + ~Builder 最终根据current_和一个或多个VersionEdit对象生成一个新的Version对象，该Version对象设置了ranges_, ranges_中的RangeMetaData的每个成员都做了初始化;
// 已持有锁R, B, S
// ok
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  // 即RangeSet中对RangeMetaData*提供的排序类
  struct BySmallestKey {
    BySmallestKey(const InternalKeyComparator* internal_comparator): internal_comparator_(internal_comparator) {
    }
    //若r1->smallest < r2->smallest 返回true; //若r1->smallest > r2->smallest 返回false
    bool operator()(const RangeMetaData* r1, const RangeMetaData* r2) const {
      int r = internal_comparator_->Compare(r1->smallest, r2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (r1->filemeta->file_number < r2->filemeta->file_number);
      }
    }
    bool operator()(const RangeInfoForEdit& r1, const RangeInfoForEdit& r2) const {
      int r = internal_comparator_->Compare(r1.Get_smallest(), r2.Get_smallest());
      assert(r != 0);
      return (r < 0);
    }
    private:
      const InternalKeyComparator* internal_comparator_;
  };

  typedef std::set<RangeMetaData*, BySmallestKey> RangeSet; //注意set类型模板的后面一个参数只是提供了比较方法
  typedef std::set<RangeInfoForEdit, BySmallestKey> RangeEditSet; //注意set类型模板的后面一个参数只是提供了比较方法
  struct LevelState { //有默认构造函数
    std::set<InternalKey> deleted_ranges; //删除的文件信息, 会插入直接删除的(在apply时插入)和被modified原来的InternalKey(在SaveTo时插入)
    RangeSet* added_ranges;             //add的文件信息 (分两部分:直接added的RangeMetaData在apply时新建,modify而added在saveTo时新建的)
    RangeEditSet* modified_rangeEdits;  //记录的是RangeInfoForEdit, 在apply时插入,作为中间结果在saveTo中使用
  };
  struct OneApply {
    LevelState levelStates[config::kNumLevels];
  };

  VersionSet* vset_;
  Version* base_;//用current_初始化
  std::vector<OneApply> applies_; //本类型只有vset,base_,applies_和cmp_四个成员
  BySmallestKey cmp_;

 public:
  // Initialize a builder with the files from *base and other info from *vset
  // 指针传递，更改有效:在VersionSet类中的调用时参数为(this, current_)
  // 因为调用本类时,已经对相关range的R锁，锁B,锁S,加锁，所以current_版本及其维护的信息不会变化
  Builder(VersionSet* vset, Version* base) 
      : vset_(vset),
        base_(base),
        cmp_(&vset_->icmp_) { //applies_调用默认构造函数,每apply一次添加一个元素
  }

  //析构applies_中的new的数组
  ~Builder() {
    for (int time = 0; time < applies_.size(); ++time) {
      for (int level = 0; level < config::kNumLevels; ++level) {
        delete applies_[time].levelStates[level].added_ranges;
        delete applies_[time].levelStates[level].modified_rangeEdits;
      }
    }
  }

  // Apply all of the edits in *edit to the current state.(*edit只作为输入), 并且更新所在VersionSet的compact指针
  // 注意apply虽然可以被调用多次,没有限制,但是一次的VersionEdit中不能包括多次不可并行的树的修改操作,因为这些修改操作可能使得modified range被多次修改,而无法知道先后顺序
  void Apply(VersionEdit* edit) {
    OneApply apply;
    for (int level = 0; level < config::kNumLevels; level++) {//设置每层的added_files（set 模板）的比较类
      apply.levelStates[level].added_ranges = new RangeSet(cmp_);  //这是std::set的构造函数，提供cmp(,)比较的方法（小于返回ture),构造一个空集
      apply.levelStates[level].modified_rangeEdits = new RangeEditSet(cmp_);  //这是std::set的构造函数，提供cmp(,)比较的方法（小于返回ture),构造一个空集
    }

    // Delete ranges(向apply中的deleted_ranges插入VersionEdit中deleted_ranges_的保存的)
    const VersionEdit::DeletedRangeSet& del = edit->deleted_ranges_;
    for (VersionEdit::DeletedRangeSet::const_iterator iter = del.begin();
         iter != del.end();
         ++iter) {
      const int level = iter->first;//caixin:<level, file number>
      const InternalKey& interKey  = iter->second;
      apply.levelStates[level].deleted_ranges.insert(interKey);
    }

    // Add new ranges,即向apply中的added_ranges插入VersionEdit中new_ranges_的保存的
    for (size_t i = 0; i < edit->new_ranges_.size(); i++) {
      const int level = edit->new_ranges_[i].first; //caixin:<level, RangeInfoForEdit>
     //Version管理的RangeMetaData都是在本函数(即Apply)或下面的SaveTo函数生成的(直接newed的RangeMetaData在apply时新建,modify而added在saveTo时新建的)
      RangeMetaData* r = new RangeMetaData(edit->new_ranges_[i].second, true, NULL);
      apply.levelStates[level].added_ranges->insert(r);
    }
    // Add modified ranges,即向levelStates中的modify_ranges_插入VersionEdit中modify_ranges_的保存的
    for (size_t i = 0; i < edit->modify_ranges_.size(); i++) {
      const int level = edit->modify_ranges_[i].first; //caixin:<level, RangeInfoForEdit>
      const RangeInfoForEdit & rangeEdit = edit->modify_ranges_[i].second;
      apply.levelStates[level].modified_rangeEdits->insert(rangeEdit);
    }
      
    applies_.push_back(apply); 
  }

  // Save the current state in *v.(将base_(调用时为current_)和要插入和删除的集合综合,并以排序的方式按序push_back(见BySmallestKey)写入v)
  //在VersionSet类中的调用时参数为一个新建的Version*
  void SaveTo(Version* v) {
    std::vector<RangeMetaData*> (&ranges) [config::kNumLevels] = v->ranges_;
    //初始,将current_的ranges信息复制给版本v;
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<RangeMetaData*>& base_ranges = base_->ranges_[level]; //base_为verstionSet中的current_
      std::vector<RangeMetaData*>::const_iterator base_end = base_ranges.end();
      for(std::vector<RangeMetaData*>::const_iterator iter = base_ranges.begin(); iter != base_end; ++iter) {
        ranges[level].push_back(*iter);
      }
    }

    std::vector<RangeMetaData*> tmp_ranges [config::kNumLevels];

    for (int time = 0; time < applies_.size(); ++time) {
      for (int level = 0; level < config::kNumLevels; ++level) {

        LevelState& levelState = applies_[time].levelStates[level];
        //根据applies_[time].levelStates[level]的modified_rangeEdits构建要新建的range(RangeMetaData)插入added_ranges和要删除的(InternalKey)插入deleted_ranges
        for (RangeEditSet::const_iterator editIter = levelState.modified_rangeEdits->begin(); editIter != levelState.modified_rangeEdits->end(); ++editIter) {
          const InternalKey& toSearch = editIter->SearchKey();//返回要在原来版本中搜索的InternalKey以定位原来的Range(在VersionSet::Builder中使用以找到原来的RangeMetaData)
          int idx = FindRange(vset_->icmp_, ranges[level], toSearch.Encode());
          assert(idx < ranges[level].size() && toSearch.Encode().ToString() == ranges[level][idx]->smallest.Encode().ToString());
          //Version管理的RangeMetaData都是在本函数SaveTo或上面的SaveTo函数生成的(直接added的RangeMetaData在apply时新建,modify而added在saveTo时新建的)
          RangeMetaData* r = new RangeMetaData(*editIter, false, ranges[level][idx]);
          levelState.added_ranges->insert(r);
          levelState.deleted_ranges.insert(toSearch); //这里插入改动过后的新版本不会添加的
        }

        std::vector<RangeMetaData*>::const_iterator base_iter = ranges[level].begin();
        std::vector<RangeMetaData*>::const_iterator base_end = ranges[level].end();
        
        //ranges[level]中将被删除的元素进行删除;向ranges[level]中添加要添加的元素;ranges[level]中会被modify的元素进行替换(删除原来的，新增替换的)
        //以上三步操作进行合并,因为是vector组织的,做一次归并排序即可得到,更加高效
        for (RangeSet::const_iterator added_iter = levelState.added_ranges->begin(); added_iter != levelState.added_ranges->end(); ++added_iter) {
          // Add all smaller files listed in base_(该循环和下面循环实现：将base_和要插入和删除的集合综合,并以排序的方式(见BySmallestKey)写入v)
          for (std::vector<RangeMetaData*>::const_iterator bpos
                 = std::upper_bound(base_iter, base_end, *added_iter, cmp_); //caixin: returns an iterator pointing to the first element in the range [base_iter,base_end) which compares greater than *adder_iter.
               base_iter != bpos;
               ++base_iter) {
            MaybeAddFile(tmp_ranges, time,  level, *base_iter, false); //对于base中存在的，若在delete集合中，则不添加
          }
  
          MaybeAddFile(tmp_ranges, time, level, *added_iter, true); //对于新添加的则一定会添加(因为可能改过后的smallest不变,所以在delete集合中有一样的smallest)
        }

        // Add remaining base files
        for (; base_iter != base_end; ++base_iter) {
          MaybeAddFile(tmp_ranges, time,  level, *base_iter, false); //对于base中存在的，若在delete集合中，则不添加
        }

        //先清空ranges(v->ranges_的引用),然后再将ranges和tmp_ranges交换内容(时间复杂度为O(1))
        ranges[level].clear();
        ranges[level].swap(tmp_ranges[level]);
      }
    }
    //将属于v的RangeMetaData的所有引用计数+1
    for(int level = 0; level < config::kNumLevels; ++level) {
      std::vector<RangeMetaData*>::const_iterator end = ranges[level].end();
      for(std::vector<RangeMetaData*>::const_iterator iter = ranges[level].begin(); iter != end; ++iter) {
        (*iter)->Ref();
      }
    }
    //注意本Builder对象中新建的但又会被删除的RangeMetaData(引用计数为0)需要析构，不是本对象新建的这里不用管,这些在对上一个版本引用计数为0时触发
    //(此流程理论上只在apply的操作可被一次完整调用中，apply会被多次进行时,某一次apply新建的再接下来apply可能被删除,因为有R锁所以不可能发生,下面的这个流程应该可以删除)
    for(int time = 0; time < applies_.size(); ++time) {
      for(int level = 0; level < config::kNumLevels; ++level) {
        RangeSet *added_ranges =  applies_[time].levelStates[level].added_ranges;
        for(RangeSet::iterator added_iter = added_ranges->begin(); //对于新添加的每一个文件(*adder_iter描述)
             added_iter != added_ranges->end();
             ++added_iter) {
          (*added_iter)->refZeroDelete(); //当引用为0时删除自己
        }
      }
    }
    

#ifndef NDEBUG
      // Make sure there is no overlap in levels > 0
    for(int level = 0; level < config::kNumLevels; ++level) {
      for (uint32_t i = 1; i < v->ranges_[level].size(); i++) {
        const InternalKey& prev_end = v->ranges_[level][i-1]->largest; //前一个的最大key
        const InternalKey& this_begin = v->ranges_[level][i]->smallest; //下一个的最小key
        if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) { //前面的不能大于等于后面的
          fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                  prev_end.DebugString().c_str(),
                  this_begin.DebugString().c_str());
          abort();
        }
      }
    }
#endif
  }

  //在属于level层的ranges中push_back一个新range r
  //若mustAdd为true，一定add, 若mustAdd为false
  //删除操作：若要插入的文件f在Builder的删除集合中，不插入（即相当于删除原来的文件f）
  //插入操作：若插入的文件f不在Builder的删除集合中，进行插入(该将要插入的新文件f的最小的key需要>= level层中已存在最大的key)
  void MaybeAddFile(std::vector<RangeMetaData*> (&ranges)[config::kNumLevels], int time, int level, RangeMetaData* r, bool mustAdd) {
    if (!mustAdd && applies_[time].levelStates[level].deleted_ranges.count(r->smallest) > 0) { //若要插入的文件f在删除集合中，不插入（即相当于删除原来的文件f）
      // File is deleted: do nothing
    } else {
      assert(ranges[level].size() == 0 || vset_->icmp_.Compare(ranges[level][ranges[level].size()-1]->largest, r->smallest) < 0);
      ranges[level].push_back(r);
    }
  }
};

//ok
VersionSet::VersionSet(const std::string& dbname,
                       const Options* options,
                       MtableCache* mtable_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      mtable_cache_(mtable_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover(), 见Recover函数的注释，第一次创建数据库时可以为2，nex_file_number_变成3
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(NULL),
      descriptor_log_(NULL),
      dummy_versions_(this), //调用了Version的私有构造函数(为双向链表的头节点，引用计数为0)
      current_(NULL),
      smallestCmp_(icmp_),
      maxStableLevelIndex_(-1),
      maxPureAddingNumForInstable_(0),
      numDoingGarbageCollection_(0),
      isAllShift_(true),
      pagesize_(sysconf(_SC_PAGESIZE)),
  	  appendAll_(true),
      maxAppendLevelIdx_(-1),
      sequenceNumTriggerMerge_(1) {
  AppendVersion(new Version(this)); //新建一个version作为current_(没有任何sst文件,Recover基于此）插入到以dummy_versions_为头节点的双向链表中(引用计数+1) 
  for(int level = 0; level < config::kNumLevels; ++level) {
    fullTask_[level] = new std::set<FullTask, BySmallest>(smallestCmp_);
    rangeReducingNum_[level] = 0;
    rangeAddingNum_[level] = 0;
    combineTodos_[level].clear();
    emptyRangeNum_[level] = 0;
    dataAmountExcludeMeta_[level] = 0;
  }
  if(options_->fixLevelandSeqs) {
    appendAll_ = false;
    maxAppendLevelIdx_ = options_->maxAppendLevelIdx;
    sequenceNumTriggerMerge_ = options_->maxSequences;
  }

}

//ok
VersionSet::~VersionSet() {
  current_->Unref();//当前版本的引用计数减一 ,若变为0，析构该version(从双向链表中删除该节点等,以及该version的所有level的所有RangeMetaData的引用减一，若引用为0删除该RangeMetaData)）
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
  for(int level = 0; level < config::kNumLevels; ++level) {
    delete fullTask_[level];
  }
}

//解引用current_,若引用计数变为0，析构该version(从双向链表中删除该节点),
//将v插入到以dummy_version为头节点的双向循环链表中，当前版本current_ = v(引用计数+1) 
//ok
void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != NULL) {
    current_->Unref();//当前版本的引用计数减一 ,若变为0，析构该version(从双向链表中删除该节点)
  }
  current_ = v;//赋值给当前版本
  v->Ref(); //引用加一

  // Append to linked list(将v插入双向链表：dummy_versions的prev_位置)
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

//调用前需已持有相关range的R锁，B锁，S锁;
//参数edit为VersionEdit*数组,数组中的各个edit中需可同时进行,否则edit中的顺序变得重要需要知道哪个先执行哪个后执行; 参数taskinfo和mutexImmR只为解任务的R锁,若不需解R锁将两者置为NULL,参数isRecoverLog默认为false,在恢复用户日志时调用需为true.
//step1:（若是第一次调用，将当前的快照（VersionSet类中的user_comparator的名字，compaction pointers 以及当前version current_的包含的所有文件的元信息）以VersionEdit序列化的格式写入一个新建的MANIFEST文件；该文件的number为versionSet的manifest_file_number_, 被Recover函数的next_file_number设置）
//step2: 解锁ms,然后将edit序列化写入MANIFEST文件,即log操作(同步写);若是第一次调用，[创建]并写CURRENT文件正确信息, 对ms加锁
//step3: edit和current_结合生成新的current_（可能引发compaction操作），即apply操作
//ok
Status VersionSet::LogAndApply(VersionEdit* *edit, const ApplyInfo* applyinfos, uint64_t editNum, port::Mutex* ms, TaskInfo** taskinfos, port::Mutex* mutexImmR, bool isRecoverLog) {
  //持有ms的锁，已lock
  //设置edit的四个数字
  for(int i = 0; i < editNum; ++i) {

     //edit[i]->log_number_ 需要设置为前面最大的一个log_number_,否则可能使得后面设置的log_number_反而更小
    if (edit[i]->has_log_number_) { //应该最多只有一个拥有
      assert(edit[i]->log_number_ >= log_number_);
      assert(edit[i]->log_number_ < next_file_number_); //next_file_number初始时为2
    } else {
      uint64_t log_number_max = log_number_;
      if(i>0) log_number_max = edit[i-1]->log_number_;
      edit[i]->SetLogNumber(log_number_max);//成员变量初始为0
    }
  
    if (!edit[i]->has_prev_log_number_) {
      edit[i]->SetPrevLogNumber(prev_log_number_);//成员变量初始为0
    } 
  
    //下面两个变量，因为多线程执行task，所以可能交叉使用这两个计数值且可能设置得偏大,但对Recover都没有有影响
    edit[i]->SetNextFile(next_file_number_); //成员变量初始为2
    edit[i]->SetLastSequence(last_sequence_);//成员变量初始为0
  } 

  //综合current_和edit 生成新的Version* v
  Version* v = new Version(this);
  {
    Builder builder(this, current_);
    for(int i = 0; i < editNum; ++i) { 
      builder.Apply(edit[i]);
    }
    builder.SaveTo(v);
  }

  // Initialize new descriptor log file即MANIFEST文件 if necessary（当打开数据库后第一次调用LogAndApply） by creating
  // a temporary file that contains a snapshot of the current version.
  // 当打开数据库后第一次调用LogAndApply时创建新的MANIFEST文件并将当前快照用VersionEdit序列化格式以log的
  //格式写入MANIFEST文件，MANIFEST文件的number为versionSet保存的manifest_file_number_。
  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == NULL) { //MANIFEST的写日志的类不存在,将创建新的Manifest文件或打开原来的manifest文件
    // No reason to unlock *ms here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(descriptor_file_ == NULL);//写日志的类关联的文件指针不存在
    if(!isRecoverLog) { //创建新的Manifest文件 new_manifest_file, 写日志的类，并写入快照值该文件
      new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_); //new_manifest_file可以为如：sunLevel/MANIFEST-000002
//      edit->SetNextFile(next_file_number_);
      s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);//创建并初始化一个新的MANIFEST文件的写日志类的文件指针。
      if (s.ok()) {
        descriptor_log_ = new log::Writer(descriptor_file_);//创建系一个新的MANIFEST文件的写日志的类
        s = WriteSnapshot(descriptor_log_);//将VersionSet类中的user_comparator的名字，compaction pointers 以及当前version current_的包含的所有文件的元信息
      }
    } else { //在恢复"user log"时，打开原来的Manifest文件dscname
      std::string current;
      Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);//文件内容读入current如"MANIFEST-000002\n"
      if (!s.ok()) {
        return s;
      }
      if (current.empty() || current[current.size()-1] != '\n') {
        return Status::Corruption("CURRENT file does not end with newline");
      }
      current.resize(current.size() - 1);//去掉最后回车,变为如"MANIFEST-000002"
      std::string dscname = dbname_ + "/" + current;//如sunLevel/MANIFEST-000002

      s = env_->NewWritableFile(dscname, &descriptor_file_, false);//打开原来的MANIFEST文件的写日志类的文件指针, 注意为"r+"的方式打开(即原来数据保留，可读可写)。
      if (s.ok()) {
        descriptor_log_ = new log::Writer(descriptor_file_);//创建写日志的类(在DBImpl::Recover函数结束时,会将descriptor_log_和descriptor_file_置为NULL)
      }
    }
  }

  // Unlock during expensive MANIFEST log write
  // 解锁
  //(将edit序列化写入MANIFEST文件，并同步),且若上面刚刚创建了MANIFESET文件，创建Current文件并写入MANIFEST文件名
  {
    ms->Unlock();

    // Write new record to MANIFEST log
    // 将edit序列化后作为一条log写入MANIFEST文件
    if (s.ok()) {
      std::string record;
      for(int i = 0; i < editNum && s.ok(); ++i) {
        edit[i]->EncodeTo(&record);
        s = descriptor_log_->AddRecord(record); //将edit序列化的内容写入该MANIFEST文件(确保已经flush至内核空间)
      }
      if (s.ok()) {
        s = descriptor_file_->Sync();//先对Manifest文件所在的目录fsync, 再对Manifest文件调用fdatasync(不影响文件的属性）以确保写入磁盘才返回
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());//Log日志无需加锁,因为FILE的所有操作的都是原子的
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
	//若刚刚创建了新的MANIFEST文件,即只在整理MANIFEST文件时使用，则（创建并）向CURREENT文件写入正确的信息。
    if (s.ok() && !new_manifest_file.empty()) {//只在刚刚创建了Manifest文件时调用:创建Current 文件
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);//向sunLevel/CURRENT文件中 写入"MANIFEST-00001\n", 确保刷入磁盘，关闭文件。若已经存在原来的被删除
    }

    ms->Lock();
  }

  // Install the new version 
  //(即Apply操作, 将v作为current_)
  
  //需要先解R锁,后面可能将旧的加了R锁的range析构，这样需要先把锁解开否则出现错误
  if (s.ok()) { //ok才需要解锁,不ok调用本函数的外部会解锁
    for(int i = 0; i < editNum; ++i) {
      if(taskinfos != NULL) {
    	  //解锁本任务的加的所有R锁,若是immutable memtable的相关任务,还释放mutexImmR锁
        taskinfos[i]->UnlockRs(mutexImmR);
        if(taskinfos[i]->tasktype() == TaskInfo::tGarbageCollection)
      	--numDoingGarbageCollection_;
      }
    }
  }

  if (s.ok()) {
    //将fullTask_中旧的满的range移除;(需要使用旧的range信息，所以不能先AppendVersion，因为这样可能会析构掉旧的range信息)
    for(int i = 0; i < editNum; ++i) {
      for(std::set<FullTask>::iterator iter = applyinfos[i].oldFulls1.begin(); iter != applyinfos[i].oldFulls1.end() ; ++iter) {
        fullTask_[applyinfos[i].oldFullLevel1]->erase(*iter);
      }
      for(std::set<FullTask>::iterator iter = applyinfos[i].oldFulls2.begin(); iter != applyinfos[i].oldFulls2.end() ; ++iter) {
        fullTask_[applyinfos[i].oldFullLevel2]->erase(*iter);
      }
    }
    AppendVersion(v); //此时MANIFEST文件一定有以前的关于current_快照，以及(可能多个)edit的序列化写入的内容,而v是所有的一个综合
    log_number_ = edit[editNum-1]->log_number_;
    prev_log_number_ = edit[editNum-1]->prev_log_number_;

    for(int i = 0; i < editNum; ++i) {
      for(std::set<InternalKey>::iterator iter = applyinfos[i].newFulls.begin(); iter != applyinfos[i].newFulls.end() ; ++iter) {
        int level = applyinfos[i].newFullLevel;
        int idx = FindRange(icmp_, current_->ranges_[level], iter->Encode()); // Return the smallest index i such that ranges[i]->largest >= iter->Encode().
        assert(current_->ranges_[level][idx]->smallest.Encode() == iter->Encode() );
        FullTask fullTask(current_->ranges_[level][idx]);
        fullTask_[level]->insert(fullTask);
      }
      if(applyinfos[i].addingNum.first >=0) 
        rangeAddingNum_[applyinfos[i].addingNum.first] -= applyinfos[i].addingNum.second;
      if(applyinfos[i].reducingNum.first >=0)
        rangeReducingNum_[applyinfos[i].reducingNum.first] -= applyinfos[i].reducingNum.second;
        maxPureAddingNumForInstable_ -= applyinfos[i].maxPureAddingNumForInstable;
        assert(rangeAddingNum_[applyinfos[i].addingNum.first] >=0 && \
             (applyinfos[i].reducingNum.first < 0 || rangeReducingNum_[applyinfos[i].reducingNum.first] >=0 ) && maxPureAddingNumForInstable_>=0);
      //设置emptyRangeNum数组
      emptyRangeNum_[applyinfos[i].oldEmptyLevel1] -= applyinfos[i].oldEmptyNum1;
      emptyRangeNum_[applyinfos[i].oldEmptyLevel2] -= applyinfos[i].oldEmptyNum2;
      emptyRangeNum_[applyinfos[i].newEmptyLevel] += applyinfos[i].newEmptyNum;
      //设置dataAmountExcludeMeta_
      dataAmountExcludeMeta_[applyinfos[i].dataDeltaLevel1] += applyinfos[i].dataDeltaAmount1;
      dataAmountExcludeMeta_[applyinfos[i].dataDeltaLevel2] += applyinfos[i].dataDeltaAmount2;
    }
  } else {//若上面写某一个文件时发生错误:写CURRENT文件或MANIFEST文件
    delete v;
    if (!new_manifest_file.empty()) {//若第一次写新的MANIFEST文件
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = NULL;
      descriptor_file_ = NULL;
      env_->DeleteFile(new_manifest_file);
    }
  }

  return s;
}

//Recover()函数:读CURRENT指向的MANIFEST文件的所有记录(VersionEdit的序列化)进行综合，构造出一个version，作为当前版本(插入版本的双向链表中) 
//并更新全部计数值(5个计数值 初始为next_file_number_(2), manifest_file_number_(0), last_sequence_(0),  log_number_(0), prev_log_number_(0),):
  //根据最后一个edit记录的值做改变（manifest_file_number_为next_file_number,next_file_number要+1),若最后一个edit的对应的计数值未设置，根据上一条edit的值设置,以此类推，都没有 返回no ok status。
//this的compact的指针为最后一个一条记录(VersionEdit的序列化)的值
//ok
Status VersionSet::Recover() {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t bytes, const Status& s) { //只设置LogReporter中的*(this->status) 为s
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);//文件内容读入current如"MANIFEST-000002\n"
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size()-1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);//去掉最后回车,变为如"MANIFEST-000002"

  std::string dscname = dbname_ + "/" + current;//如sunLevel/MANIFEST-000002
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);

  { //为了自动回收log::Reader的资源
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true/*checksum*/, 0/*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) { //循环读一个MANIFEST文件的每一条记录，并利用Apply函数将所有的记录综合至builder
      VersionEdit edit;
      s = edit.DecodeFrom(record); //从MANIFEST的一条记录中反序列化至edit
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
       //将edit的内容综合值builder
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_; //根据最新反序列化的edit更新以下4个值
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_; //初始时为2(即数据库刚创建时从2赋值，第一个edit的对应值)
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }//循环结束
  }
  delete file;
  file = NULL;

  if (s.ok()) {
    if (!have_next_file) { //这三个数字，一个versionEdit写入的是一定存在的(见NewDB函数)
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    Version* v = new Version(this);
    //将current_和前面的builder的关于的每个level的文件排好序写入v
    builder.SaveTo(v);
    
    // Install recovered version
    //将v插入到以dummy_version为头节点的双向链表中，当前版本current_ = v 等
    AppendVersion(v);
    //更新VersionSet即this中的计数值:根据最后一个edit记录的值做改变（manifest_file_number_为next_file,next_file_number要+1) 
    manifest_file_number_ = next_file; 
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    //初始化fullTask_队列数组,maxStableLevelIndex_, emptyRangeNum_[], dataAmountExcludeMeta_[]
    std::vector<RangeMetaData*> (&ranges)[config::kNumLevels] = current_->ranges_;
    for(int level = 0; level < config::kNumLevels; ++level) {
      int64_t size = ranges[level].size();
      if(size >= StandardTableNumForLevel(level) )
        maxStableLevelIndex_ = level;
      for(int64_t i = 0 ; i < size; ++i) {
        if(ranges[level][i]->isFull()) {
          fullTask_[level]->insert( fullTask_[level]->end(), FullTask(ranges[level][i])); //插入到最后,时间复杂度O(1)
        }
        else if(ranges[level][i]->appendTimes == 0) {
          assert(ranges[level][i]->DataMount() == 0);
          ++emptyRangeNum_[level];
        }
      }
      dataAmountExcludeMeta_[level]= NumLevelBytes(level, false);
    }
    //为levelCachedInfos_中可能会存储数据的层分配空间(若之前没有数据,只为第一层分配空间)
    for(int level = 0; level <= maxStableLevelIndex_ + 1; ++level) {
      bool success = levelCachedInfos_[level].setCapacity(StandardTableNumForLevel(level));
      assert(success);
      levelCachedInfos_[level].setMaxPairNum(ranges[level].size() < StandardTableNumForLevel(level) ? ranges[level].size() : StandardTableNumForLevel(level));
    }
   
  }

  return s;
}

// Mark the specified file number as used.
// 即使next_file_number为number的下一个：number+1
// ok
void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

//将VersionSet类中的user_comparator的名字，compaction pointers 以及当前version current_的包含的所有文件的元信息
//先写入VersionEdit对象中，然后再序列化写入log中(确保已经flush至内核空间)
//ok
Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata(写至edit)
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save ranges(将当前version即current_中的所有range的信息RangeMetaData写至edit的new_ranges_)
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<RangeMetaData*>& ranges = current_->ranges_[level];
    for (size_t i = 0; i < ranges.size(); i++) {
      const RangeMetaData* r = ranges[i];
      RangeInfoForEdit rangeinfo(*r);
      edit.AddNewRange(level, rangeinfo);
    }
  }
   
  //将edit的内容序列化并写入log文件
  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

//返回第level层 current_记录的文件个数
//ok
int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->ranges_[level].size();
}

//返回字符串：每一层,文件的个数
//ok
const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  assert(config::kNumLevels == 7);
  snprintf(scratch->buffer, sizeof(scratch->buffer),
           "ranges[ %d %d %d %d %d %d %d ]",
           int(current_->ranges_[0].size()),
           int(current_->ranges_[1].size()),
           int(current_->ranges_[2].size()),
           int(current_->ranges_[3].size()),
           int(current_->ranges_[4].size()),
           int(current_->ranges_[5].size()),
           int(current_->ranges_[6].size()));
  return scratch->buffer;
}

std::string VersionSet::DebugFullRangeString() const {
  std::string result;
  for(int i = 0; i < config::kNumLevels; ++i ) {
    result += "level ";
    AppendNumberTo(&result, i);
    result += " : ";
    for(std::set<FullTask, BySmallest>::const_iterator cst_iter = fullTask_[i]->begin();  cst_iter != fullTask_[i]->end(); ++cst_iter) {
      result += "[";
      result += cst_iter->rg->smallest.DebugString();
      result += "]";
    }
    if(!fullTask_[i]->empty())
      result += "\n";
    else
      result.erase(result.size()-10, 9); //前一个参数是索引
  }
  return result;
}

//所有version的所有SSTable文件number加入live(一个set) ,参数addPercent0File表示是否将usepercent为0即还没新建的文件加入
//可能percent==0也得添加进去: 因为其他线程可能正在新建该文件，新建出来就被本线程给删除了
//ok
void VersionSet::AddLiveFiles(std::set<uint64_t>* live, bool addPercent0File) {
  for (Version* v = dummy_versions_.next_;
       v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<RangeMetaData*>& ranges = v->ranges_[level];
      for (size_t i = 0; i < ranges.size(); i++) {
        assert(ranges[i]->filemeta != NULL);
        if(addPercent0File || ranges[i]->usePercent != 0) //若==0表示未使用则没有分配文件
          live->insert(ranges[i]->filemeta->file_number); //不会重复，因为是set
      }
    }
  }
}

void VersionSet::SetDescriptorLogNULL() {
  delete descriptor_log_;
  delete descriptor_file_;
  descriptor_log_ = NULL;
  descriptor_file_ = NULL;
}

//需已经持有S锁
//设置levelCachedInfos_, totalCachedDataRatio_, 返回值为false表示数据库的数据量很少,不推荐merge
bool VersionSet::setDataInfo(port::Mutex& mutexS) {

  std::vector<RangeMetaData*> (*ranges) [config::kNumLevels] = &current_->ranges_;
  //若存储的数据或抽样的数据不足，则不进行merge(不设置)
  int totalRangeNum = 0;
  for(int i = 0; i <= maxStableLevelIndex_+1; ++i) {
    totalRangeNum += (*ranges)[i].size();
  }
  if(isAllShift_ || totalRangeNum <= config::rgNumThresholdForMerge || totalRangeNum <= config::samplingRgNum) {
    return false;
  }

  //(已持有S锁)对抽样的Range加R锁后,解S锁,再获取缓存信息(此时因为持有RangeMetaData的R锁,其在未来的版本中不会发生变化,故释放S锁可减少S锁的持有时间),获取完后解R锁, 再加S锁更新levelCachedInfos_, dataAmountExcludeMeta_, totalCachedDataRatio_
  
  //本抽样方法暂时对关闭数据库后再打开的情况不太适用(所以需要打开数据库时完整做一遍缓冲比较好; 对于内存有很大波动如清缓存的情况,可能需要更有效的方法)
  int idx = random()%totalRangeNum;
  int levelIdx;
  bool success = getIdxInOneLevel(idx, levelIdx);
  assert(success);
  std::vector<RangeMetaData*> lockedRanges;
  for(int i = 0; i < config::samplingRgNum; ++i) {
    if((*ranges)[levelIdx][idx]->appendTimes > 0 && (*ranges)[levelIdx][idx]->filemeta->range_lock.Trylock()) //没有数据的无需,因为添加进levelCachedInfos_也无意义
      lockedRanges.push_back((*ranges)[levelIdx][idx]);
    else
      break;
    idx = (idx + 1) % (*ranges)[levelIdx].size();
  }
  int finalLevelIdx = maxStableLevelIndex_+1;
  if(finalLevelIdx == levelIdx) {
    levelCachedInfos_[finalLevelIdx].setMaxPairNum((*ranges)[finalLevelIdx].size() < StandardTableNumForLevel(finalLevelIdx) ? (*ranges)[finalLevelIdx].size() : StandardTableNumForLevel(finalLevelIdx));
  }
  mutexS.Unlock(); //解锁以减少对S锁的持有时间, 接下来只访问的共享信息为:加了R锁的RangeMetaData
  ranges = NULL; //解锁S锁后current_->ranges_可能会发生改变(若后面引用必须先加锁再重新赋值)

  std::vector<MyPair> cachedinfos;
  int fd = -1;
  int open_flags = O_RDONLY | O_NOATIME; //设置O_NOATIME是为尽量减少磁盘操作, 但是一些文件系统如NFS可能不支持
  for(int i = 0; i < lockedRanges.size(); ++i) {
    std::string path = MtableFileName(dbname_, lockedRanges[i]->filemeta->file_number);
    //先打开文件
    fd = open(path.c_str(), open_flags, 0);
    if (fd == -1 && errno == EPERM && (open_flags & O_NOATIME != 0) ) {
        open_flags &= ~O_NOATIME;
        fd = open(path.c_str(), open_flags, 0);
    }
    assert(fd != -1);
    int total_pages = 0, total_pages_in_core = 0, total_pages_not_in_core = 0;

    //这里统计的都不包含索引数据，因为索引数据是额外被缓存的
    if(lockedRanges[i]->offset1 > lockedRanges[i]->index_offset_end) { //MSStable的超出阈值的格式
      file_in_RAM(fd, 0,                                 lockedRanges[i]->index_offset_end - lockedRanges[i]->holeSize, pagesize_, total_pages, total_pages_in_core, total_pages_not_in_core);
      file_in_RAM(fd, lockedRanges[i]->index_offset_end, lockedRanges[i]->offset1 - lockedRanges[i]->index_offset_end, pagesize_, total_pages, total_pages_in_core, total_pages_not_in_core);
    }
    else { //MSStable正常格式或SStable格式
      file_in_RAM(fd, 0, lockedRanges[i]->offset1, pagesize_, total_pages, total_pages_in_core, total_pages_not_in_core);
    }
    assert(total_pages == total_pages_in_core + total_pages_not_in_core);
    cachedinfos.push_back(std::make_pair(total_pages, total_pages_in_core));
    close(fd);
    lockedRanges[i]->filemeta->range_lock.Unlock();
  }
 
  mutexS.Lock(); //重新对S加锁
  //将抽样得到的缓存信息插入levelCachedInfos_[levelIdx]
  for(int i = 0; i < cachedinfos.size(); ++i) {
    levelCachedInfos_[levelIdx].insertPair(cachedinfos[i]);
  }

  int sampledRangeNum = 0;
  for(int level = 0; level <= maxStableLevelIndex_+1; ++level) {
    sampledRangeNum += levelCachedInfos_[level].getPairNum();
  }
  if(sampledRangeNum <= config::rgNumThresholdForMerge) {
    return false;
  }

// 设置totalCachedDataRatio_

  MyPair sum;
  for(int level = 0; level <= maxStableLevelIndex_ + 1; ++level) {
    MyPair levelSum = levelCachedInfos_[level].setSum();
    sum.first += levelSum.first;
    sum.second += levelSum.second;
  }
  totalCachedDataRatio_ = double(sum.second)/sum.first;
  assert(totalCachedDataRatio_ <= 1);

  return true;
}


//需已经持有S锁
//设置appendAll_, maxAppendLevelIdx_, int sequenceNumTriggerMerge_;
void VersionSet::setParametersForMerge(int splitNum, bool suggestIntegrateMerge) {

  appendAll_ = true;
  maxAppendLevelIdx_ = -1;
  sequenceNumTriggerMerge_ = 1;
  if(isAllShift_ || !suggestIntegrateMerge) {
    return;
  }

  double finalLevelTimes = double(current_->ranges_[maxStableLevelIndex_+1].size()) / current_->ranges_[maxStableLevelIndex_].size() + 1; //额外加1以使得下面循环正常工作

  int64_t totalDataAmount = 0;
  int levelNum = maxStableLevelIndex_ + 2;
  assert(levelNum <= config::kNumLevels);

  //初始化 dataRatio[];
  double dataRatio[config::kNumLevels]; //每一层总的数据量占全部数据量的比例
  for(int i = 0; i < levelNum; ++i) {
    totalDataAmount += dataAmountExcludeMeta_[i];
  }
  for(int i = 0; i < levelNum; ++i) {
    dataRatio[i] = double(dataAmountExcludeMeta_[i])/totalDataAmount;
  }


  double lastLevelsDataRatio = 0; //遍历的上面所有层的数据占总的数据量的比例
  for(int i = 0; i < levelNum; ++i) {
    if(lastLevelsDataRatio + dataRatio[i] <= totalCachedDataRatio_) {
      maxAppendLevelIdx_ = i;
      lastLevelsDataRatio += dataRatio[i];
    }
    else {
      appendAll_ = false;
      if(i <= maxStableLevelIndex_) { //内部层
        for(int k = 2; k <= config::levelTimes+1; ++k) {
          if(lastLevelsDataRatio + dataRatio[i]*((k-1.0)/config::levelTimes) <= totalCachedDataRatio_)
            sequenceNumTriggerMerge_ = ((k+1)/2 > 2 ? (k+1)/2 : 2); //取"k/2向上取整"是为预留一部分内存给读操作,对"2"优先是因为其对降低写放大高效,向上取整是因为取”3“时写放大下降也较可观,且因还有数据层，多一点内存缓存对读性能也有限
          else
            break;
        }
        return;
      }
      else { //最后一层
        for(int k = 2; k <= finalLevelTimes + 1; ++k) {
          if(lastLevelsDataRatio + dataRatio[i]*((k-1.0)*splitNum/(splitNum+1)/finalLevelTimes) <= totalCachedDataRatio_)
            sequenceNumTriggerMerge_ = ((k)/2 > 2 ? (k)/2 : 2); //取"k/2"时是为预留一部分内存给读操作,对"2"优先是因为其对降低写放大高效; 不向上取整是因为取"3"写放大降低得内部层明显,且下面没有数据层了,多一些内存缓存对读性能提高有较大作用
          else
            break;
        }
        return;
      }
    }
  }

}
 
//需已经持有S锁
//根据options设置的固定值设置appendAll_, maxAppendLevelIdx_, sequenceNumTriggerMerge_;
void VersionSet::mayResetParametersForMerge() {
  if(options_->fixLevelandSeqs) {
    appendAll_ = false;
    maxAppendLevelIdx_ = options_->maxAppendLevelIdx;
    sequenceNumTriggerMerge_ = options_->maxSequences;
  }
}


//返回current_记录的第level层的所有文件存储的数据量数之和, includeMeta的默认值为ture
//ok
int64_t VersionSet::NumLevelBytes(int level, bool includeMeta) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->ranges_[level], includeMeta);
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
// inputs为输入，smallest,largest为输出：inputs描述的所有文件的IternalKey的范围
// ok
void VersionSet::GetRange(const std::vector<RangeMetaData*>& inputs,
                          InternalKey* smallest,
                          InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    RangeMetaData* r = inputs[i];
    if (i == 0) {
      *smallest = r->smallest;
      *largest = r->largest;
    } else {
      if (icmp_.Compare(r->smallest, *smallest) < 0) {
        *smallest = r->smallest;
      }
      if (icmp_.Compare(r->largest, *largest) > 0) {
        *largest = r->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
// 设置inputs1，inputs2合并后的smallest InternalKey和largest InternalKey
// ok
void VersionSet::GetRange2(const std::vector<RangeMetaData*>& inputs1,
                           const std::vector<RangeMetaData*>& inputs2,
                           InternalKey* smallest,
                           InternalKey* largest) {
  std::vector<RangeMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());//在all.end()前面插入inputs2.begin() inputs2.end()之间的所有内容
  GetRange(all, smallest, largest);
}


namespace {
static void CleanupFullIter(void* arg1, void* arg2) {
  std::vector<RangeMetaData*>* nextOverlap_merges = reinterpret_cast<std::vector<RangeMetaData*> * >(arg1);
  delete nextOverlap_merges;
}
}
//所有类型(除了shift类型外)即flush和split的任务都能调用本函数
//将imm/inputs_[0]与inputs_[1]满的range生成的归并迭代器返回
//ok
Iterator* VersionSet::MakeInputIterator(TaskInfo* taskinfo) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;
  Iterator** list = new Iterator*[2]; //flush到非稳定层的为两个, shift不能调用本函数，其他的如正常的flush和split为1个
  int num = 0;
  if(taskinfo->imm != NULL) {
    list[num++] = taskinfo->imm->NewIterator();

  } else {
    assert(taskinfo->inputs_[0].size() == 1);
    if(taskinfo->inputs_[0][0]->usePercent > 0) {
      list[num++] = mtable_cache_->NewIterator(options,
                                              taskinfo->inputs_[0][0]->filemeta->file_number,
                                              taskinfo->inputs_[0][0]->index_offset_end, 
                                              taskinfo->inputs_[0][0]->index_offset,
                                              taskinfo->inputs_[0][0]->appendTimes);
    }
    else {
      list[num++] = NewEmptyIterator();
    }
  }

//  //只有对flush到非稳定层 且 父亲有数据 的情况 构造满的孩子的迭代器(与setVersionEdit函数和SetApplyinfo对old full的设置一致)
//  if(taskinfo->isFlushToInstable  && (taskinfo->isMemTask() || taskinfo->inputs_[0][0]->usePercent>0) ) {
//    std::vector<RangeMetaData*>* nextOverlap_fulls = new std::vector<RangeMetaData*>;
//    bool hasToIter = false;
//    for(int i = 0; i < taskinfo->inputs_[1].size(); ++i) {
//      if(taskinfo->inputs_[1][i]->isFull()) {
//        hasToIter = true;
//        nextOverlap_fulls->push_back(taskinfo->inputs_[1][i]);
//      }
//    }
//    if(hasToIter) {
//      list[num++] = NewTwoLevelIterator(  //第一层迭代器,生成file number,index_offset_end,index_offset(根据key，找到哪个文件中包含)
//        new Version::LevelFileNumIterator(icmp_, nextOverlap_fulls),
//        &GetFileIterator, mtable_cache_, options);          //生成第二层迭代器(利用file_value获得Table_Cache的内部迭代器)函数,和函数中的两个参数
//      //为第二个迭代器(最多两个)注册析构函数以使其在析构时删除new的空间
//      list[num-1]->RegisterCleanup(CleanupFullIter, nextOverlap_fulls, NULL);
//    } else {
//      delete nextOverlap_fulls;
//    }
//  }

  //只有对flush到非稳定层 且 父亲有数据 的情况 构造满的孩子的迭代器(与setVersionEdit函数和SetApplyinfo对old full的设置一致)
  if(taskinfo->isFlushTask() && (taskinfo->isMemTask() || taskinfo->inputs_[0][0]->usePercent>0) ) {
    std::vector<RangeMetaData*>* nextOverlap_merges = new std::vector<RangeMetaData*>;
    bool hasToIter = false;
    for(int i = 0; i < taskinfo->inputs_[1].size(); ++i) {
      if(taskinfo->inputs_[1][i]->isFull() || taskinfo->isChildMerge[i] ) {
		assert(!taskinfo->inputs_[1][i]->isFull() || (taskinfo->inputs_[1][i]->isFull() && taskinfo->isFlushToInstable)); //孩子满的一定是flush到最后一层的任务
        hasToIter = true;
        nextOverlap_merges->push_back(taskinfo->inputs_[1][i]);
      }
    }
    if(hasToIter) {
      list[num++] = NewTwoLevelIterator(  //第一层迭代器,生成file number,index_offset_end,index_offset(根据key，找到哪个文件中包含)
        new Version::LevelFileNumIterator(icmp_, nextOverlap_merges),
        &GetFileIterator, mtable_cache_, options);          //生成第二层迭代器(利用file_value获得Table_Cache的内部迭代器)函数,和函数中的两个参数
      //为第二个迭代器(最多两个)注册析构函数以使其在析构时删除new的空间
      list[num-1]->RegisterCleanup(CleanupFullIter, nextOverlap_merges, NULL);
    } else {
      delete nextOverlap_merges;
    }
  }

  Iterator* result = NULL;
  if(num==1){
    result = list[0];
  }
  else if(num ==2) {
    result = NewMergingIterator(&icmp_, list, num);   //list[0],list[1]归并生成的迭代器返回
  }
  else  assert(0);

  delete[] list;
  return result;
}

//下面的三种类型用于对层进行优先级排序
namespace {
  typedef std::pair<int, int64_t> LevelAndDiff;  //存储<level, diff>
  struct Bysmall { //LevelDiffInfos的排序规则:若返回true表示a排在前面,否则b排在前面
    Bysmall() {
    }
    bool operator() (const LevelAndDiff& a, const LevelAndDiff& b) const {
      assert(a.first>=0 && b.first>=0 &&  a.first!=b.first);
      if(a.second > MaxDiff(a.first)) return true;  //对于超过最大值的层排在最前面（只在非稳定层转换为稳定层时出现）
      else if(b.second > MaxDiff(b.first)) return false;  //对于超过最大值的层排在最前面（只在非稳定层转换为稳定层时出现）
      else if(a.first==0 && a.second > 0)  return true;        //第一层，若diff>0,排在其次
      else if(b.first==0 && b.second > 0)  return false;  //第一层，若diff>0,排在其次
      else if(a.second == b.second ) return a.first < b.first; //对于range数超出的个数相等的(包括都超出0的)，上层的排在前面
      else{ //个数大的排在前面
        return a.second > b.second;
      }
    }
  };
  typedef std::set<LevelAndDiff, Bysmall> LevelDiffInfos;
}

//已经加了锁S,选取要执行的任务包括所有优先级(包括Immutable Memtable的下移的任务), 此过程中S锁一直持有,不会/不能释放再加(因为版本的信息可能会发生变化使不一致)
//选取的任务的相关锁R会被持有, 返回的对象需要用户释放内存
TaskInfo* VersionSet::PickTask(MemTable* imm, port::Mutex* mutexImmR) {
  TaskInfo* taskinfo = NULL;
  const Comparator* ucmp = icmp_.user_comparator();//默认返回为 BytewiseComparatorImpl *
  std::vector<RangeMetaData*> (&ranges)[config::kNumLevels] = current_->ranges_;
  #ifndef NDEBUG
  VerifyInfos();
  #endif
  //可能更新maxStableLevelIndex_(层数增长)
  if(maxStableLevelIndex_ < config::kNumLevels-1 && ranges[maxStableLevelIndex_+1].size() >= StandardTableNumForLevel(maxStableLevelIndex_+1)) {
    if (maxPureAddingNumForInstable_ != 0) {  //确保非稳定层的所有的在执行的range数增长任务结束后才能使层数增长
      return NULL; //等待该变量为0,此期间后台线程不执行新任务
    }
    ++maxStableLevelIndex_;         //可以使得层数的自动增长
    //对内部层设置maxPairNum,最后一层的设置在setDataInfo函数中
    levelCachedInfos_[maxStableLevelIndex_].setMaxPairNum(StandardTableNumForLevel(maxStableLevelIndex_));
    //对最后一层设置capacity
    bool success = levelCachedInfos_[maxStableLevelIndex_+1].setCapacity(StandardTableNumForLevel(maxStableLevelIndex_+1));
    assert(success);
  }

  int targetLevel = -1;
  std::vector<FullTask> blockedImmTasks;
  //优先级1: 若immuable Memtable存在且可以直接下移数据则构造Imm下移任务, 否则进行优先级2
  if ( imm != NULL ) {
    if (maxStableLevelIndex_ == -1) { //还没有稳定层
      //对于level0即非稳定层,若其个数>=阈值不会执行这里(因为上面已经转换为稳定层或直接返回, 其实即使进入本函数也返回NULL任务)
      taskinfo = ConstructTaskInfo(NULL, imm, mutexImmR, targetLevel, false); //Imm的combine-shift或combine-flush
      assert(taskinfo == NULL || ranges[0].size() < StandardTableNumForLevel(0)); //若选取任务成功则level 0的个数一定是小于阈值的
    } else { //已有稳定层
      InternalKey smallest, largest;
      GetImmRange(imm, smallest, largest);  //获取Imm的范围 smallest和largest
      GetOverlappingTasks(icmp_, *fullTask_[targetLevel+1], &smallest, &largest, &blockedImmTasks);
      if(blockedImmTasks.size() == 0) {
        int immDownLevel = current_->PickLevelForMemTableOutput(smallest.user_key(), largest.user_key());
        assert(immDownLevel==0 || ranges[immDownLevel-1].size() >= StandardTableNumForLevel(immDownLevel-1));
	    if (ranges[immDownLevel].size() <= MaxTableNumForLevel(immDownLevel) ) //以优先使树的结构符合要求(这里其实无需判断,构造任务时会进行更严格的判断)
          taskinfo = ConstructTaskInfo(NULL, imm, mutexImmR, targetLevel, false); //Imm的combine-shift或combine-flush
      }
    }
  }

  //上面因为锁或者range数或有满的range阻塞Imm下移, 则执行下面

  //优先级2:若上面Imm下移任务选取失败(因为锁，或不能直接下移),进行combine任务以调整树的结构优先, 若没有任务或任务选择失败，进行优先级3
  if (taskinfo == NULL) {
    //首先构造优先级排好序信息，用于指导combine操作
    Bysmall cmp;
    LevelDiffInfos levelDiffs(cmp);
    for (int level = 0; level <= maxStableLevelIndex_; ++level) {
      int64_t  rangeNumLeastDiff =  WillLeastNumRange(level)- StandardTableNumForLevel(level);
      levelDiffs.insert(LevelAndDiff(level, rangeNumLeastDiff ) );
    }
    //条件iter->second >= (MaxDiff+1)/2 表示range数-正在减少的range数要大于等于阈值与最大值的平均数时开始处理
      //且 后一个条件说明要combine次优层需满足最优的层的range数没有超过最大值(实际上最多只有非稳定层因被flush且最后转换为稳定层时才可能大于最大值,
      //因为其他线程正在处理该层而使本线程不能再处理该层)，只有该层降到最大值及以下才可以进行其他任务(保持树的结构)
    int lastLevel = -1;
    for(LevelDiffInfos::const_iterator iter = levelDiffs.begin(); taskinfo == NULL && iter != levelDiffs.end() && iter->second >= (MaxDiff(iter->first)+1)/2 \
                                                               && ( lastLevel == -1 || ranges[lastLevel].size() <= MaxTableNumForLevel(lastLevel) ); ++iter) {
      int targetLevel = iter->first;
      lastLevel = targetLevel;
      //对于非稳定层，在添加后需要满足的range个数小于等于"阈值";对于稳定层，在添加之后需要满足其range个数小于等于"最大值"
      int canAddMostRangeNum= CanAddMostRangeNum(targetLevel+1);
      //获得maxDiff, 其值为: shift操作可下移的最大ragne数,且下层的个数对于稳定层不超过最大的值,对于非稳定层不超过阈值
      int maxDiff = iter->second <= canAddMostRangeNum ? iter->second : canAddMostRangeNum;  //取两者的最小值
      std::vector<RangeMetaData*> &rges = ranges[targetLevel];
      if(maxDiff <= 0) continue; //因为下层的个数加正在增加的等于最大值,跳过该层(为了简化combine-flush也不进行)
  
      std::vector<int64_t> shiftIndex;
      std::vector<unsigned char> overlapNums;
      std::vector<unsigned char> combinedChildNums;
      getChildsInfo(targetLevel, maxDiff, shiftIndex, overlapNums, combinedChildNums);
      int zeroAndLockNum = shiftIndex.size();
      if(zeroAndLockNum > 0) { //shift task, 所有参与的锁已经加上
        assert(zeroAndLockNum <= maxDiff);
        taskinfo = new TaskInfo(targetLevel, TaskInfo::tCombineShift);
        //将shiftIndex中的任务构造shift任务
        for(std::vector<int64_t>::const_iterator iter = shiftIndex.begin(); iter != shiftIndex.end(); ++iter) {
          taskinfo->inputs_[0].push_back(rges[*iter]);
        }
        rangeReducingNum_[targetLevel] += zeroAndLockNum;
        rangeAddingNum_[targetLevel+1] += zeroAndLockNum;
      }
      else { //combine-flush task
        int64_t targetIdx = -1; //要combine-flush的range的索引
        double point = -1000;
        //若记录了其他线程未完成的任务, 因为可能已经发生了改变,所以还需重新判断条件是否满足,若不满足需要重新选择最优的进行combine-flush
        if(!combineTodos_[targetLevel].smallest.isEmpty()) {
          targetIdx = FindRange(icmp_, rges, combineTodos_[targetLevel].smallest.Encode());
          bool pointValid;
          point = getPoint(rges[targetIdx], overlapNums[targetIdx], combinedChildNums[targetLevel], &pointValid);
          if( !pointValid || point < combineTodos_[targetLevel].point ) {
            targetIdx = -1;
            combineTodos_[targetLevel].clear();
          }
        }
  
        if(targetIdx == -1) {
          //利用overlapNums信息构造combine任务
          double maxPoint = -1000; //下面的评分约在[-1,1]之间
          assert(rges.size() > 2);
          for(int i = 1; i < rges.size()-1; ++i) {
            bool pointValid;
            //获得rges[i]的进行combine-flush的评分返回,若该range满足combine的前提条件,*pointValid设为true,否则设为false
            point = getPoint(rges[i], overlapNums[i], combinedChildNums[i], &pointValid);
            if(pointValid && point > maxPoint) {
              maxPoint = point;
              targetIdx = i;
            }
          }
          point = maxPoint;
        }
  
        if (targetIdx!= -1) { //当某一层特别是第一层刚变成稳定层，所有range被加锁而使得无法被处理(shift操作) 且 全部range都没有孩子所有point都无效 则可以-1
          // Store task in "overlapTasks" in fulltask_ that overlap [smallest,largest],注意函数内部实际比较的是user key
          std::vector<FullTask> overlapTasks;
          GetOverlappingTasks(icmp_,*fullTask_[targetLevel+1], &rges[targetIdx]->smallest, &rges[targetIdx]->largest, &overlapTasks);
           //无需等待,在最大稳定层的满的任务(看作叶子)或下面没有满的或待处理的range没有数据,构造combine任务(一般为combine-flush,shift还是可能的其有一个非常小的时间窗口:在其他线程最后S锁释放仍持有R锁时此split任务不能被上面的选取而执行到这该Range的R锁被其他线程释放,不可能为split)处理
          if( targetLevel == maxStableLevelIndex_ || overlapTasks.size() == 0 || rges[targetIdx]->DataMount()==0 ) {
            taskinfo = ConstructTaskInfo(rges[targetIdx], NULL, NULL, targetLevel, true); //此处构造的是combine-flush任务(只有此处最后一个参数为true)
            if(taskinfo == NULL) continue; //因为锁或本层range个数或下一层个数的原因而不能构建任务, 则对次优层进行处理
            assert(taskinfo->type_ == TaskInfo::tCombineFlush);
            combineTodos_[targetLevel].clear(); //执行的可能是combineTodos_记录的, 对其复位
          }
          else{ //需要先对下层的满的任务进行处理
            for(int i=0; taskinfo == NULL && i < overlapTasks.size(); ++i) { //对每个孩子递归调用本函数
              //构造处理满的任务返回(combine-shift/data-shift/ajustment-flush/split)
              taskinfo=  FindLeafTask(overlapTasks[i], targetLevel+1);
            }
            if(taskinfo == NULL ) continue;
            combineTodos_[targetLevel].set(rges[targetIdx]->smallest, point); //重新设置combineTodos_
          }
        }
      }
    }
  }
  #ifndef NDEBUG
  for(int level = 0; level < maxStableLevelIndex_-1; ++level) { //只可能最后两层超过最大值，且一但超过不会再构造下面类型的任务
    assert(ranges[level].size() <= MaxTableNumForLevel(level));
  }
  #endif
  //若上面的combine操作还没来得急使新生成的稳定层的range个数满足要求，此时不再获取优先级低的其他任务(树的结构需要优先满足: 以不因为持有锁而阻塞combine操作)
  if(taskinfo == NULL && ranges[maxStableLevelIndex_].size() > MaxTableNumForLevel(maxStableLevelIndex_)) { 
    return NULL;
  }

  //优先级3:若上面选择任务失败 选择处理满的range, 若没有任务或任务选择失败，则继续优先级4(若因为层数不满足要求则直接返回NULL)
    //若level 1是稳定层且有阻塞Immutable Memtable下移的满的range优先

   //不能通过isAllShift_使得本判断过程略过第一层的判断(只可略过不影响树运行的任务)
   //否则可能出问题的有：1.当打开一个已经存在的数据库时isAllShift_被初始化为ture,使得Imm_下移而第一层有满阻塞的任务无法获取而出现永久阻塞;2,从顺序写转换为随机写也可能需要同样的问题
  if( blockedImmTasks.size() > 0 ) { 
    int blockedNum = blockedImmTasks.size();
    int count = 0;
    for (int i = random()%blockedNum; taskinfo == NULL && i < blockedImmTasks.size() && count < blockedNum; i = (++i % blockedNum), ++count) {
      assert(maxStableLevelIndex_ >= 0);
      taskinfo =  FindLeafTask(blockedImmTasks[i], 0); //构造处理满的任务返回(adjustment-flush/split/data-shift)
    }
  }
  if(taskinfo == NULL && !isAllShift_) { //full task(为了提高顺序写性能，顺序写下面无需判断)
    //再对每一层的满的任务为根的树随机选出满的任务处理(注意非稳定层不属于该树), 顺序写不执行本步骤以免影响顺序写性能;
    for(int level = 0; taskinfo==NULL && level < 2 && level <= maxStableLevelIndex_ && fullTask_[level]->size() > 0; ++level) {
      int fullNum = fullTask_[level]->size();
      int idx = random()%fullNum;
      std::set<FullTask>::iterator iter = fullTask_[level]->begin();
      while(--idx >= 0) {
        ++iter;
      }
      int tryTimes = fullNum;
      for( ; taskinfo == NULL && tryTimes > 0; ++iter,--tryTimes) {
		if(fullTask_[level]->end() == iter) {
          iter = fullTask_[level]->begin();
        }
        taskinfo =  FindLeafTask(*iter, level);//构造处理满的任务返回(adjustment-flush/split/data-shift)
      }
    }
  }

//能进行下面的任务构造时，树的每一层的range数必已经满足结构( standard <= 个数 <= max)
  //优先级4-1:增加多线程的并行度( 同时也有读优化功能和弱的垃圾回收功能)
  //每层最多尝试一次：若加锁不成功就尝试下一层; 
  //对于单线程只对第一层处理; 对于多线程,当level 3(第四层)的文件个数超过level 2(第三层)的阈值,如1000(总的约为128G+,若leveltimes为12,则约为216G+),则只对前两层进行并行处理.
  for(int level = 0; taskinfo==NULL && !isAllShift_ && level <= maxStableLevelIndex_ && ( (config::backThreadNum == 1 && level == 0) || ( config::backThreadNum > 1 && (level < 2 || ranges[3].size() <= StandardTableNumForLevel(2)) ) || config::affordLongAdjustTimeAfterLoad); ++level) {
    int rangeNum = ranges[level].size();
    int startIdx = random()%rangeNum; //用随机数控制，增加多线程的并行度
    int64_t maxDataMount = 0;
    int maxIdx = -1;
    int count = 0;
    int iterMaxTimes = config::levelTimes; //每层迭代的个数,即使下面存在满足条件但是没有找到也没关系
    for(int i = startIdx; count < rangeNum && count < iterMaxTimes; i=(++i % rangeNum), ++count){
      if(ranges[level][i]->DataMount() > maxDataMount) { //若在读写混合的场景中,找出的为有较大数据量的,所以对Imm的下移的最终效果影响应该不大
        maxDataMount = ranges[level][i]->DataMount();
        maxIdx = i;
        if(maxDataMount >= config::fileCommonSize*0.85) break; //以防止找到的都是正在处理的满的，这样不能实现附加功能: 增加多线程的并行度
      }
    }
    if(maxIdx != -1 && maxDataMount >= config::fileCommonSize * 0.75) {
      assert(ranges[level][maxIdx]->DataMount() > 0);
      FullTask tmp(ranges[level][maxIdx]);
      taskinfo = FindLeafTask(tmp, level);
    }
  }

  //优先级4-2: 读优化(同时也有弱的垃圾回收功能)
  //若options_->strictRO为true(默认为true),对所有的稳定层进行读的优化操作
  for (int i = 0; taskinfo == NULL && !isAllShift_ && i < options_->numMaxRangesForRO && current_->range_to_compact_[i] != NULL; ++i ) {
    //因为记录的是"读取超过两个range时所取的第一个range"且读取的是最新版本的才可见,所以不可能是非稳定层的range; 对于数据量为0的allowed_seeks被设置为无穷
    assert(current_->range_to_compact_level_[i] <= maxStableLevelIndex_ && maxStableLevelIndex_ >= 0 && current_->range_to_compact_[i]->DataMount() > 0);
    FullTask tmp(current_->range_to_compact_[i]); //生成一个新版本后这两个数组便被重置了
    taskinfo = FindLeafTask(tmp, current_->range_to_compact_level_[i]);
  }

  //优先级4-3:若options_->numMaxThreadsDoingGC>=1,对最后一层(需大于等于第二层)进行垃圾回收(同时也有读优化功能,但显然开销较大)
    //后台线程同时进行垃圾回收的个数应小于options_->numMaxThreadsDoingGC(默认为1, 打开垃圾回收);
  if(taskinfo==NULL && !isAllShift_ && numDoingGarbageCollection_ < options_->numMaxThreadsDoingGC && maxStableLevelIndex_ >= 0 && ranges[maxStableLevelIndex_+1].size() > 0) {
    int totalStableEmptyRgNum = 0, totalStableRgNum = 0;
    for(int i = 0;  i <= maxStableLevelIndex_; ++i) {
      totalStableEmptyRgNum+= emptyRangeNum_[i];
      totalStableRgNum+= ranges[i].size();
    }
    assert(totalStableRgNum> 0);
    if(totalStableEmptyRgNum/double(totalStableRgNum) >= 0.80) {
      int level = maxStableLevelIndex_+1;
      int rangeNum = ranges[level].size();
      const int iterMaxTimes = ranges[maxStableLevelIndex_+1].size();
      int startIdx =  random() % rangeNum; // "%"运算符需要后面因子>0,条件中已经满足
      int minIdx = -1, count = 0;
      int minAllowed_seek =  ranges[level][startIdx]->allowed_seeks;
      for(int i = startIdx; taskinfo==NULL && count < iterMaxTimes; i=(++i % rangeNum), ++count) {
        if(ranges[level][i]->appendTimes >= 2 && ranges[level][i]->allowed_seeks <= minAllowed_seek) { //只对appendTimes >=2的进行处理
          minAllowed_seek = ranges[level][i]->allowed_seeks;
          minIdx = i;
          if(minAllowed_seek <= 0) { //数据量>=3/4的阈值或appendTimes大于levelTimes时为0(因为为非稳定层，不可能为负数)
            break;
          }
        }
      }
      if(minIdx != -1) {
        //若因为锁或非稳定层的个数返回NULL,则选取任务失败(此时必定有其他线程在进行,否则非稳定层已转化为稳定层,所以不用担心无任务而所有线程睡眠的情况)
        assert(ranges[level][minIdx]->appendTimes >= 2);
        taskinfo = getTaskForGarbage(ranges[level][minIdx], level);
        if(taskinfo != NULL) ++numDoingGarbageCollection_;
      }
    }
  }
 
  if(taskinfo != NULL && isAllShift_ && taskinfo->tasktype() != TaskInfo::tCombineShift && taskinfo->tasktype() != TaskInfo::tDataShift) {
    isAllShift_ = false;
  }

  if(!isAllShift_)
    SetIsChildMerge(taskinfo); 

  return taskinfo;
}

//输入: //targetLevel表示要进行task的层的索引
        //maxDiff, 其值为: shift操作可下移的最大ragne数,且下层的个数对于稳定层不超过最大的值,对于非稳定层不超过阈值,其大于0才会调用本函数
//输出：
  // shiftIndex表示被加R锁成功且可以shift的range，其个数<=maxDiff, 当为空时下面两个输出才完整(才有意义)
  //overlapNums[i]表示该层的索引为i的range的孩子数; combinedChildNums[i]表示该层的索引为i的range combined后相邻兄弟大约的最大平均孩子数(根据上面两个得到)
void VersionSet::getChildsInfo(int targetLevel, int maxDiff, \
                               std::vector<int64_t>& shiftIndex, \
                               std::vector<unsigned char>& overlapNums, \
                               std::vector<unsigned char>& combinedChildNums) {
  assert(maxDiff > 0);
  std::vector<unsigned char> freeChildNums;   //freeChildNums表示该层的游离的孩子数(元素个数比overlapNums多1)
  std::vector<RangeMetaData*> &rges = current_->ranges_[targetLevel];
  std::vector<RangeMetaData*> &next_rges = current_->ranges_[targetLevel+1];
  const Comparator* ucmp = icmp_.user_comparator();//默认返回为 BytewiseComparatorImpl *
  int zeroAndLockNum = 0;
  //获取targetlevel的每一个range与下一层相交的range个数的数组overlapNums
    //一旦获得了maxDiff个孩子数为0且加锁成功的就停止获取,因为对于顺序写中的shift操作经常发生,所以需要控制复杂度;
    //在随机写场景中combine-flush操作发生频率小，所以控制较为放松(不在RangeMetaData中记录孩子数,虽然这样时间复杂度小,但是记录较为复杂)
    
  // Return the smallest index such that next_rges[index]->largest >= rges[0].small.Encode(),如果不存在返回next_rges.size(), 通过userkey比较
  int index = FindRange(icmp_, next_rges, rges[0]->smallest.Encode(), true);
  freeChildNums.push_back(index);
  bool currentIdxMayFree = false; //初始值没关系，因为对于下面i等于0时不会使用
  for(int i = 0; i < rges.size() && zeroAndLockNum < maxDiff; ++i) {
    //对于不同的rges[i], 使得index的含义与循环传进来时一样
      //get the smallest index  such that next_rges[index]->largest >= rges[i].small.Encode(),如果不存在返回next_rges.size(), 通过userkey比较
    int freeChildNum = 0;
    while( index < next_rges.size() && icmp_.InternalKeyComparator::Compare(next_rges[index]->largest, rges[i]->smallest)  < 0 ) {
      if(!currentIdxMayFree) { //当前的不可能为free,因为已经属于rges[i-1]的孩子了(对于i=0的freeChildNum不在本循环内设置)
        currentIdxMayFree =  true; //因为下一个range可能为free的,所以设置为true
      } else {                //确定为free的, 因为其不属于rges[i-1]的孩子又不属于rges[i]: 对freeChildNum++， currentIdxFree保持true
        freeChildNum++;
      }
      index++;
    }
    if(i != 0) freeChildNums.push_back(freeChildNum);

    if(index == next_rges.size()) { //无相交
      if(rges[i]->filemeta->range_lock.Trylock()) { //若加锁成功
        shiftIndex.push_back(i);
        zeroAndLockNum++;
      }
      overlapNums.push_back(0);
      continue;
    }
    for(int j = index; j < next_rges.size(); ++j ) {
      Slice user_key= rges[i]->largest.user_key();
      if( BeforeRange(ucmp, &user_key, next_rges[j])) { //与索引为j的range无相交
        if(j == index) { //说明rges[i]与next_rges没有一个有相交且加锁成功
          if(rges[i]->filemeta->range_lock.Trylock()) { //加锁成功
            shiftIndex.push_back(i);
            zeroAndLockNum++;
          }
          overlapNums.push_back(0);
          currentIdxMayFree =  true; //index对应的range不属于rges[i]故可能free: 若属于rges[i+1]的孩子,此变量可为任意值因为freeChildNums++的循环不会执行; 若不属于则需要设置为true使得freeChildNums++
        }
        break; //一旦无相交就会跳出循环
      } else {
        //rges[i]与next_rges[j]相交
        if(j == index)  overlapNums.push_back(1);
        else          overlapNums[i]++;
        index = j; //若rges[i]有孩子,循环结束后index为本range的最后一个孩子的索引; 否则index不变
        currentIdxMayFree = false; //index对应的range已经属于rges[i]的孩子(当然也可能属于rages[i+1]的孩子),故设置为false
      }
    }
  }

  if(zeroAndLockNum == 0) { //即全部遍历完, 设置freeChildNums的最后一个元素和设置完combinedChildNums
    if(currentIdxMayFree)  // next_rges[index]不属于rges[]中的最后一个range, 即free
      freeChildNums.push_back(next_rges.size() - index);
    else
      freeChildNums.push_back(next_rges.size() -1 - index);
    assert(overlapNums.size() == rges.size() && rges.size() >= 2 && freeChildNums.size() == rges.size() + 1);
    //根据overlapNums和freeChildNums设置combinedChildNums
    for(int i = 0; i < overlapNums.size(); ++i) {
      if(i == 0) {
        combinedChildNums.push_back(freeChildNums[i] + overlapNums[i] + freeChildNums[i+1] + overlapNums[i+1]); //设置了但不使用
      }
      else if(i == overlapNums.size() - 1) {
        combinedChildNums.push_back(overlapNums[i-1] + freeChildNums[i] + overlapNums[i] + freeChildNums[i+1]); //设置了但不使用
      }
      else {
        combinedChildNums.push_back(ceil((overlapNums[i-1] + freeChildNums[i] + overlapNums[i] + freeChildNums[i+1] + overlapNums[i+1])/2.0));
      }
    }
  }
  //若zeroAndLockNum==0， overlapNums和freeChildNums已经设置完毕
}

//获得range的进行combine-flush的评分返回,若该range满足combine的前提条件，*pointValid设为true，否则设为false
//参数overlapNum表示range与下一层的ranges相交的个数, combinedChildNum表示range combine后相邻两兄弟节点的大概孩子数
double getPoint(const RangeMetaData* range, unsigned char overlapNum, unsigned char combinedChildNum, bool *pointValid) {
  if (overlapNum <= 0) { //=0的情况为:存在combine-shift的range(孩子数为0)但被其他线程加锁处理而不能进行combine-shift,剩下的只能被combine-flush，此时对该层的判断会出现=0的情况
    *pointValid = false;
    return 0;
  }
  assert(overlapNum > 0);
  // 数据量,越大越好以较好的消除随机写，比重为0.5(最多约为0.5), 若没有数据,设为一个较大的值1.0
  double dataAmountPart = range->DataMount() == 0 ?  1.0 : 0.5 * range->DataMount()*config::levelTimes/double(overlapNum)/config::fileCommonSize;
  // 此range没有后, 与相邻的一个或两个range平摊的孩子数越小越好，占的比重为1
  double childNumPart = -1;
  if( combinedChildNum <=  (config::levelTimes*3)/2 ) {
    childNumPart = combinedChildNum / double(config::levelTimes);
    *pointValid = true;
  } else {
    *pointValid = false;
  }
  return dataAmountPart - childNumPart;
}

//构造任务(flush/ combine-shift/data-shift /split)而调用前需确保 targetLevel为最大的稳定层或目的range的下层没有相交的满的range 或 range没有数据(对于combine-flush时可能触发); 
//参数targetLevel表示要进行操作的range所在的层，对于imm为-1,对于磁盘上的range需大于等于0小于等于最大的稳定层(对于垃圾回收任务的获取不在本函数)
//参数isFlushForCombine为ture表示若为磁盘上的flush任务则为combine任务，否则为对于磁盘上的flush任务则为adjustment-flush; 对于非flush任务isFlushForCombine应该设置为false(其实虽然两者都可以)
//对在索引为targetLevel的层的range构造任务返回(new得到的):
  //首先对range要加锁成功;
  //然后对于shift/split/flush到非稳定层的任务要保证被增加的层的range个数不大于最大值(对于flush到非稳定层，其个数不大于阈值),要减少的层的个数大于阈值;对于shift操作若可以减少range数则为combine-shift若不可以减少则为data-shift;对于flush,若isFlushForCombine为true则为combine-flush(所以,个数因满足要求)否则为adjustment-flush
  //对于flush操作，将所有被覆盖的range也要加锁成功,对任意一条条件不满足释放完已获得的资源后(包括锁和内存),返回NULL;
TaskInfo* VersionSet::ConstructTaskInfo(RangeMetaData * range, MemTable* imm, port::Mutex* mutexImmR, int targetLevel, bool isFlushForCombine) {
  assert( !range && imm && mutexImmR || range && !imm && !mutexImmR );
  if(imm != NULL) assert(targetLevel == -1);
  if(range != NULL) assert(targetLevel >= 0);

  TaskInfo* taskinfo = NULL;
  int shiftDownLevel = targetLevel+1; //只对imm的shift有效
  if( range != NULL && range->filemeta->range_lock.Trylock() || imm!=NULL && mutexImmR->Trylock() ) { //对range/imm尝试加锁成功
    std::vector<RangeMetaData*> overlapRanges;
    if(range != NULL) {
      current_->GetOverlappingInputs(targetLevel+1, &range->smallest, &range->largest, &overlapRanges); //比较的是user key
    }
    else if(imm != NULL) {
      InternalKey firstKey, lastKey;
      GetImmRange(imm, firstKey, lastKey);
      current_->GetOverlappingInputs(targetLevel+1, &firstKey, &lastKey, &overlapRanges); //比较的是user key
      shiftDownLevel = current_->PickLevelForMemTableOutput(firstKey.user_key(), lastKey.user_key());
    }

    TaskInfo::TaskType type = TaskInfo::tInvalid;
    if (overlapRanges.size() == 0 && CanAddRange(shiftDownLevel)) { //shift操作
      if (ShouldReduceRange(targetLevel)) { //对于Imm的shift一定为tCombineShift
        type = TaskInfo::tCombineShift;
        if(targetLevel >= 0)
          rangeReducingNum_[targetLevel] ++;
      } else {
        type = TaskInfo::tDataShift;
      }
      rangeAddingNum_[shiftDownLevel] ++;
    } else if(overlapRanges.size() > 0 && (overlapRanges.size() < config::splitTriggerNum || isFlushForCombine) &&  //对于combine-flush操作无需判断孩子数
              (targetLevel != maxStableLevelIndex_ || CanAddRange(targetLevel+1)) ) { //flush操作, 对于flush到非稳定层仍判断下层是否可以增加range
      if(imm != NULL || isFlushForCombine){ //combine任务(Imm flush定为tCombineFlush)
        assert(ShouldReduceRange(targetLevel));
        type = TaskInfo::tCombineFlush;
      }
      else {
        assert(imm == NULL);
        type = TaskInfo::tAdjustmentFlush;
      }
    } else if(overlapRanges.size() >= config::splitTriggerNum && CanAddRange(targetLevel) && !isFlushForCombine) { //split操作 (对于Imm,不可能为该类型)
      assert(targetLevel != -1);
      type = TaskInfo::tSplit;
      rangeAddingNum_[targetLevel] ++; 
    } else {                      //flush到非稳定层，下层可能不能再被flush range而生成新的range了，不成功
      if(range!=NULL) range->filemeta->range_lock.Unlock();
      else if(imm!=NULL) mutexImmR->Unlock();
      return NULL;
    }
    assert(type != TaskInfo::tInvalid);
    
    taskinfo = new TaskInfo(targetLevel, type);    assert(taskinfo!=NULL);
    if(range!=NULL)
      taskinfo->inputs_[0].push_back(range); //flush/shift/split ,其中split和shift已经不用再设置了
    else 
      taskinfo->imm = imm;

    if(taskinfo->type_ == TaskInfo::tAdjustmentFlush || taskinfo->type_ == TaskInfo::tCombineFlush) { //flush
      int i = 0;
      for( ; i < overlapRanges.size(); ++i) {
        if(!overlapRanges[i]->filemeta->range_lock.Trylock())  break;
        taskinfo->inputs_[1].push_back(overlapRanges[i]);
      }
      if(i != overlapRanges.size()) { //没有将所有的相关锁加成功,  故跳过该任务
        for(--i ; i>=0; --i){ //解锁,注意先--i,因为索引为i的不加锁成功
          overlapRanges[i]->filemeta->range_lock.Unlock();
        }
        delete taskinfo;
        if(range!=NULL) range->filemeta->range_lock.Unlock();
        else if(imm!=NULL) mutexImmR->Unlock();
        return NULL;
      }

      if(targetLevel == maxStableLevelIndex_ ) { //flush到非稳定层
        taskinfo->maxPureAddingNumForInstable = getAdding_NewNumForInstable(imm, range, overlapRanges, taskinfo->maxNewedNumForInstable);
        taskinfo->isFlushToInstable = true;
        maxPureAddingNumForInstable_ += taskinfo->maxPureAddingNumForInstable;
      }
    } else if(taskinfo->type_ == TaskInfo::tSplit) {
      int childNum = overlapRanges.size();
      assert(childNum >= config::splitTriggerNum);
      taskinfo->divideUserkeyForSplit =  icmp_.user_comparator()->MiddleString(overlapRanges[childNum/2 -1]->largest.user_key(), \
                                                                               overlapRanges[childNum/2]->smallest.user_key() );
    } else if(imm != NULL && taskinfo->type_ == TaskInfo::tCombineShift) {
      taskinfo->immShiftLevel= shiftDownLevel;
    }

    if(taskinfo != NULL && taskinfo->type_ == TaskInfo::tCombineFlush && targetLevel >=0)
      rangeReducingNum_[targetLevel] ++;
  }

  return taskinfo;
}

TaskInfo* VersionSet::getTaskForGarbage(RangeMetaData * range, int targetLevel) {
  assert(targetLevel == maxStableLevelIndex_+1 && range != NULL);
  TaskInfo* taskinfo = NULL;
  if(CanAddRange(targetLevel)) {
    if(range->filemeta->range_lock.Trylock()) { //对range尝试加锁成功
      taskinfo = new TaskInfo(targetLevel, TaskInfo::tGarbageCollection);    assert(taskinfo!=NULL);
      taskinfo->inputs_[0].push_back(range);
      std::vector<RangeMetaData*> overlapRanges; //只为了传递参数，无其他作用
      taskinfo->maxPureAddingNumForInstable = getAdding_NewNumForInstable(NULL, range, overlapRanges, taskinfo->maxNewedNumForInstable);
  
      maxPureAddingNumForInstable_ += taskinfo->maxPureAddingNumForInstable;
    }
  }
  return taskinfo;
}


//调用本函数的task参数不一定是"满的"
//深度遍历找出以task(在索引为level的层中,不需要为满的range)为根,与fullTask_有相交关系的为孩子的树(注意非稳定层不属于该树)的 未被加锁的第一个叶子任务返回,
//根据不同的任务(combine-shift/data-shift/ajustment-flush/split,对于split/shift若增加个数的层的个数超出最大值则跳过)，其相关锁也被加锁成功;
//若不存在这样的任务则返回NULL;
TaskInfo* VersionSet::FindLeafTask(const FullTask& task, int level) {
  assert(level<=maxStableLevelIndex_);
  TaskInfo* taskinfo = NULL;
  std::vector<FullTask> overlapTasks;
  GetOverlappingTasks(icmp_, *fullTask_[level+1], &task.rg->smallest, &task.rg->largest, &overlapTasks);
  if(level == maxStableLevelIndex_ ||  overlapTasks.size() == 0 ) { //处理的层为最大的稳定层或其他稳定层叶子(虽然对于split操作不需要这个条件但是因为split发生频率低这样更简化)
    return ConstructTaskInfo(task.rg, NULL, NULL, level, false); //ajustment-flush/split/combine-flush/data-shift任务，非combine-flush任务
  } 
  else { //为非叶子
    for(int i = 0; taskinfo==NULL && i < overlapTasks.size(); ++i) { //对每个处于稳定层的孩子递归调用本函数
      assert(level < maxStableLevelIndex_);
      taskinfo =  FindLeafTask(overlapTasks[i], level + 1);
      //若为NULL，则continue;
    }
  }
  return taskinfo;
}

//对于flush操作,设置taskinfo->isChildMerge以指导孩子进行merge还是append
void VersionSet::SetIsChildMerge(TaskInfo* taskinfo) {
  if( taskinfo != NULL && taskinfo->isFlushTask() ) {
    std::vector<RangeMetaData*> childs = taskinfo->inputs(1);
    int childsNum = childs.size();
    for(int i = 0; i < childsNum; ++i) {
      if(options_->integratedAppendMerge && !appendAll_ && (taskinfo->level() + 1 > config::leastAppendLevel) \
         && (taskinfo->level() + 1 > maxAppendLevelIdx_ + 1 \
             || (taskinfo->level() + 1 == maxAppendLevelIdx_ + 1 && childs[i]->appendTimes >= sequenceNumTriggerMerge_ ) )) {  //taskinfo->level()+1 表示孩子节点所在的层
        assert(maxAppendLevelIdx_ >= -1 && sequenceNumTriggerMerge_ >= 1); //若appendAll_为false需要满足的条件
        taskinfo->isChildMerge.push_back(true); 
      }
      else { 
        taskinfo->isChildMerge.push_back(false); 
      }
    }
    assert(taskinfo->isChildMerge.size() == childs.size());
  }
}

//idx既为输入也为输出,levelIdx为输出：根据range的编号，输出该range所在的levelIdx和在该层的idx
bool VersionSet::getIdxInOneLevel(int& idx, int& levelIdx) {
  int tmpIdx = idx;
  levelIdx = -1;
  while(tmpIdx >= 0) {
    idx = tmpIdx;
    levelIdx++;
    tmpIdx -= current_->ranges_[levelIdx].size();
    assert(current_->ranges_[levelIdx].size() > 0 && levelIdx < config::kNumLevels);
  }
  return (levelIdx != -1);
}

void VersionSet::VerifyInfos() {
  std::vector<RangeMetaData*> (&ranges)[config::kNumLevels] = current_->ranges_;
  //验证fullTask_正确
  for(int level = 0; level <= maxStableLevelIndex_+1; ++level) {
    std::set<FullTask, BySmallest>::const_iterator cst_iter = fullTask_[level]->begin();
    int idx = 0;
    for(; cst_iter != fullTask_[level]->end() && idx < ranges[level].size(); ++cst_iter, ++idx){
       if(ranges[level][idx]->isFull()) {
         if(ranges[level][idx] != cst_iter->rg) {
           assert(0);
         }
       } else {
         while(idx+1 < ranges[level].size() && !ranges[level][idx+1]->isFull()) //找到下一个满的，需必定存在
           ++idx;
         ++idx;
         assert(idx < ranges[level].size() && ranges[level][idx]->isFull() && ranges[level][idx] == cst_iter->rg);
       } 
    }
    assert(cst_iter == fullTask_[level]->end());
    for(; idx < ranges[level].size(); ++idx) {
       if(ranges[level][idx]->isFull()) {
         assert(0);
       }
    }
  }
  //验证emptyRangeNum_正确
  for(int level = 0; level <= maxStableLevelIndex_+1; ++level) {
    int num = 0;
    for(int i = 0; i < ranges[level].size(); ++i) {
      if(ranges[level][i]->DataMount() == 0)
        num++;
    }
    if(emptyRangeNum_[level] != num) {
      assert(0);
    }
  }
  //验证dataAmountExcludeMeta_ 
  for(int i = 0; i < config::kNumLevels; ++i) {
    int64_t levelBytes =  NumLevelBytes(i,false);
    assert(dataAmountExcludeMeta_[i] == levelBytes);
  }
}

//对于type_ == tCombineFlush ||  tCombineShift || == tSplit || tGarbageCollection操作，inputs_[0]的记录的range插入edit的deleted ranges集合
//对于有数据的range flush到非稳定层上，inputs_[1]中满的range 或 需要被归并的孩子 将插入edit的deleted ranges集合(与MakeInputIterator 和 SetApplyinfo函数一致)
void TaskInfo::AddInputDeletions(VersionEdit* edit) {
  if(type_ == tCombineFlush || type_ == tCombineShift || type_ == tSplit || type_ == tGarbageCollection) {
    for(size_t i = 0; i < inputs_[0].size(); ++i) {
      edit->DeleteRange(level_, inputs_[0][i]->smallest);
    }
  }
//  if(isFlushToInstable && (isMemTask() || inputs_[0][0]->usePercent>0) ) { //对于有数据的range flush到 非稳定层上，满的孩子将被删除
//    for(size_t i = 0; i < inputs_[1].size(); ++i) {
//      if(inputs_[1][i]->isFull()) {
//        edit->DeleteRange(level_ + 1, inputs_[1][i]->smallest);
//      }
//    }
//  }
  if(isFlushTask() && (isMemTask() || inputs_[0][0]->usePercent>0) ) { //对于有数据的range flush到 非稳定层上，满的孩子将被删除; 或需要被归并的孩子将被删除
    for(size_t i = 0; i < inputs_[1].size(); ++i) {
      if(inputs_[1][i]->isFull() || isChildMerge[i] ) {
        edit->DeleteRange(level_ + 1, inputs_[1][i]->smallest);
      }
    }
  }

}









}  // namespace branchdb
