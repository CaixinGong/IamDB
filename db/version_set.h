//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_BRANCHDB_DB_VERSION_SET_H_
#define STORAGE_BRANCHDB_DB_VERSION_SET_H_

#include <map>
#include <set>
#include <vector>
#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include "db/memtable.h"
#include "branchdb/env.h"
#include "util/pair_FIFO_pool.h"

namespace branchdb {

namespace log { class Writer; }

class TaskInfo;
class Iterator;
class MtableBuilder;
class MtableCache;
class Version;
class VersionSet;
class WritableFile; //暂时保留，后面可能需改


//记录着满的range的类
struct FullTask {
  RangeMetaData* rg;
  FullTask(RangeMetaData* range): rg(range) { }
};
 //用户创建set<FullTask> 或 set<InternalKey> 时使用的排序类; 在VersionSet中创建了一个对象以供类中的set<FullTask> 或 set<InternalKey>使用
class BySmallest {
 public:
  BySmallest(const InternalKeyComparator& icmp): icmp_(icmp) { }
  bool operator() (const FullTask& task1, const FullTask& task2) const {
     return icmp_.Compare(task1.rg->smallest, task2.rg->smallest)<0;
  }
  bool operator() (const InternalKey& interKey1, const InternalKey& interKey2) const {
     return icmp_.Compare(interKey1, interKey2)<0;
  }
 private:
  InternalKeyComparator icmp_;
};

//LogAndApply中的Apply使用的部分信息
struct ApplyInfo {
  ApplyInfo(const BySmallest& smallestCmp): addingNum(-1,0), reducingNum(-1,0), \
                                            oldFullLevel1(-1), oldFullLevel2(-1), oldFulls1(smallestCmp), oldFulls2(smallestCmp), \
                                            newFullLevel(-1), newFulls(smallestCmp), maxPureAddingNumForInstable(0), \
                                            oldEmptyLevel1(0), oldEmptyLevel2(0), newEmptyLevel(0), oldEmptyNum1(0), oldEmptyNum2(0), newEmptyNum(0),\
                                            dataDeltaLevel1(0), dataDeltaLevel2(0), dataDeltaAmount1(0), dataDeltaAmount2(0) {
  }
  std::pair<int, int> addingNum;       //level 和 正在增加的数目(只对shift和split操作有效, flush到非稳定层而增加的除外)
  std::pair<int, int> reducingNum;     //level 和 正在减少的数目(只对shift和非Imm的combine-flush操作有效)
  int oldFullLevel1, oldFullLevel2;       //旧的满的range属于哪一层(有效值>=0)
  std::set<FullTask, BySmallest> oldFulls1, oldFulls2; //原满的range(现在已经不满或不存在),属于同一层的比较操作通过BySmallest()
  int newFullLevel;                       //新生成的满的range属于哪一层(有效值>=0)
  std::set<InternalKey, BySmallest> newFulls; //新生成的满的range,属于同一层的, 比较操作通过BySmallest(),为了更容易的构建其类型为InternalKey(记录smallest)
  int maxPureAddingNumForInstable;    //只对flush任务且flush到非稳定层  和 最后一层的range垃圾回收 有效
  int oldEmptyLevel1, oldEmptyLevel2, newEmptyLevel;
  int oldEmptyNum1, oldEmptyNum2, newEmptyNum;
  int dataDeltaLevel1, dataDeltaLevel2;        //无数据的combine-fush操作这里不应该设置
  int64_t dataDeltaAmount1, dataDeltaAmount2; //不包括"元素据"的变化
};

// Return the smallest index i such that ranges[i]->largest >= key.
// Return ranges.size() if there is no such range.
// REQUIRES: "ranges" contains a sorted(排序的) list of non-overlapping(不相互重叠的) ranges.
//输入的ranges一般是某一level的,采用二分查找法
extern int FindRange(const InternalKeyComparator& icmp,
                    const std::vector<RangeMetaData*>& ranges,
                    const Slice& key,
                    bool isByUserkey = false); //默认是通过InternalKey寻找的

// Returns true iff some ranges in "ranges" overlaps the user key range
// [*smallest,*largest].
// smallest==NULL represents a key smaller than all keys in the DB.
// largest==NULL represents a key largest than all keys in the DB.
//判断ranges中是否存在与[smallest_user_key,largest_user_key](不是InternalKey)有overlap, 若有:返回 true
extern bool SomeRangeOverlapsRange(
    const InternalKeyComparator& icmp,
    const std::vector<RangeMetaData*>& ranges,
    const Slice* smallest_user_key,
    const Slice* largest_user_key);

//对于imm或range下移到非稳定层，其在非稳定层相交的range数组为overlapRanges，获得其下移最多可能生成的range的个数
static int getMaxAddingNumForInstable(MemTable* imm, RangeMetaData* range, std::vector<RangeMetaData*> &overlapRanges);

//获得ranges的进行combine-flush的评分返回,若该range满足combine的前提条件，*pointValid设为true，否则设为false
//参数overlapNum表示range与下一层的ranges相交的个数, combinedChildNum表示range下移后兄弟节点大概的孩子数
double getPoint(const RangeMetaData* range, unsigned char overlapNum, unsigned char combinedChildNum, bool *pointValid);

class Version {
 public:
  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  //向(*iters)中加入某一个version中的所有迭代器, 迭代器的数量为：若该level 不空，则每个level一个迭代器
  //本函数将被DBimpl类中与memTable, immutable memtable(若有的话), 创建一个mergeing迭代器返回,并进一步处理internal key等格式
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  // Lookup the value for key.  If found(用了TableCache的内部私有函数，再seek后进行了比较，key相等才算found), store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held
  //注意stats填充：当读某个user key, 读了超过一次文件时设置该stats,记录了寻找的第一个文件的信息，函数未读memtable(因为version内未组织)
  struct GetStats {  //在下面Get函数中作为返回值
    RangeMetaData* seek_range;
    int seek_range_level;
  };
  Status Get(const ReadOptions&, const LookupKey& key, std::string* val,
             GetStats* stats);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  // REQUIRES: lock is held
  // 函数过程：若stats记录的文件遇到的次数已经到达了一个阈值则根据该stats设置file_to_compact_ file_to_compact_level_两个成员变量并且返回true
  //          用来指导后台线程task任务的选取，是一种读的优化
  //RecordReadSample使用了UpdateStats函数
  bool UpdateStats(const GetStats& stats);

  // Record a sample(取样) of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod(为1024*1024)
  // bytes.  Returns true if a new compaction may need to be triggered.
  // REQUIRES: lock is held
  //函数过程：根据key(internalkey),若overlap了该user key的文件有两个或以上, 设置该第一个文件的Getstats类型对象,再调用UpdataStats(见上面描述）
  //可能设置file_to_compact_ file_to_compact_level_两个成员变量。若有两个文件以上overlap了user key则返回true(该函数设置了最多两次）
  bool RecordReadSample(Slice key);
  
  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void Ref();    
  void Unref(); //析构函数为私有，在该函数里面delete对象使用以实现复制控制

  // Store in "*inputs" all files in "level"(指定的level) that overlap [begin,end]
  void GetOverlappingInputs(
      int level,
      const InternalKey* begin,         // NULL means before all keys
      const InternalKey* end,           // NULL means after all keys
      std::vector<RangeMetaData*>* inputs,
      std::pair<int,int>* twoEndIdx = NULL);

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==NULL represents a key smaller than all keys in the DB.
  // largest_user_key==NULL represents a key largest than all keys in the DB.
  //若指定level,有与 [*smallest_user_key,*largest_user_key]重叠的文件，则返回true
  bool OverlapInLevel(int level,
                      const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  // 详细分析见.cc文件
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);
  
  //返回指定level的range数
  int NumRanges(int level) const { return ranges_[level].size(); }

  
  const std::vector<RangeMetaData*> (&ranges()) [config::kNumLevels] {
    return ranges_;
  }

  // Return a human readable string that describes this version's contents(全部的level的全部文件的描述信息).
  std::string DebugString() const;

 private:
  friend class Compaction;
  friend class VersionSet;

  //见.cc文件的描述，NewConcatenatingIterator用于获得非level 0的某一level的迭代器
  class LevelFileNumIterator;
  Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

  // Call func(arg, level, r) for every range that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  //
  // REQUIRES: user portion of internal_key == user_key.
  void ForEachOverlapping(Slice user_key, Slice internal_key,
                          void* arg,
                          bool (*func)(void*, int, RangeMetaData*));

  VersionSet* vset_;            // VersionSet to which this Version belongs
  Version* next_;               // Next version in linked list
  Version* prev_;               // Previous version in linked list
  int refs_;                    // Number of live refs to this version

  // List of files per level(数组，每个元素为vecor<RangeMetaData*>,每个元素记录着每层的sstable的的信息), 生成都是通过Builder中根据归并生成的,故用vector
  std::vector<RangeMetaData*> ranges_[config::kNumLevels]; //kNumLevels默认为7

  // Next range to compact based on seek stats.如上面的RecordReadSample 和UpdateStats函数
  RangeMetaData* range_to_compact_[config::backThreadNum];
  int range_to_compact_level_[config::backThreadNum];

  //私有构造函数也是唯一的构造函数，供友元类VersionSet使用
  explicit Version(VersionSet* vset) 
      : vset_(vset), next_(this), prev_(this), refs_(0) { //引用为0，当插入双向循环链表中引用才变 
    const int elems = config::backThreadNum;
    assert(elems > 0);
    for(int i=0; i < elems; ++i) {
      range_to_compact_[i] = NULL;
      range_to_compact_level_[i] = -1;
    }
  }

  //析构函数(私有, unRef中使用)： Remove from linked list(从双向链表中删除该节点)
  // Drop references to files(该vertion的所有level的所有文件的引用减一，若引用为0删除该FileMetaData)
  ~Version();

  // No copying allowed
  Version(const Version&);
  void operator=(const Version&);
};

class VersionSet {
 public:
  VersionSet(const std::string& dbname,
             const Options* options,
             MtableCache* mtable_cache,
             const InternalKeyComparator*);
  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  // edit为VersionEdit*的数组, taskinfo为TaskInfo*数组
  Status LogAndApply(VersionEdit* *edit, const ApplyInfo* applyinfos, uint64_t editNum, port::Mutex* ms, \
                                     TaskInfo** taskinfos, port::Mutex* mutexImmR, bool isRecoverLog);
      EXCLUSIVE_LOCKS_REQUIRED(ms);

  // Recover the last saved descriptor（MANIFEST文件） from persistent storage.
  Status Recover();

  // Return the current version.
  Version* current() const { return current_; }

  // Return the current manifest file number
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  // 如file_number为要创建的文件(调用了NewFileNumber()）但是创建失败，所以要重新利用（下次调用NewFileNumber()时返回同一个数字)
  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  //返回第level层 current_记录的文件个数
  int NumLevelFiles(int level) const;

  // Return the combined file size of all files at the specified level.
  //返回current_记录的第level层的所有文件的字节数之和
  int64_t NumLevelBytes(int level, bool includeMeta = true) const;

  // Return the last sequence number.//最新的（或上一个）sequence number，即InternalKey的那个
  uint64_t LastSequence() const { return last_sequence_; }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  // Mark the specified file number as used.
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // Pick level and inputs for a new task.
  // Returns NULL if there is no task can be got.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the task.  Caller should delete the result.
  TaskInfo* PickTask(MemTable* imm, port::Mutex* mutexImmR);

  // Create an iterator that describe all data should  rewrite 
  // The caller should delete the iterator when no longer needed.
  //生成的迭代器效果与为c->inputs_[0],[1]下的需要重新写的的MSSTable merge(不是真正的merge)后在生成新的迭代器
  Iterator* MakeInputIterator(TaskInfo* taskinfo);

  // May also mutate some internal state.
  //所有version的所有SSTable文件number加入live(一个set)
  void AddLiveFiles(std::set<uint64_t>* live, bool addPercent0File);

  void SetDescriptorLogNULL();

  //设置levelCachedInfos_, totalCachedDataRatio_, 返回值为false表示数据库的数据量很少,不推荐merge
  bool setDataInfo(port::Mutex& mutexS);

  //设置appendAll_, maxAppendLevelIdx_, int sequenceNumTriggerMerge_;
  void setParametersForMerge(int splitNum, bool suggestMerge);

  //根据options设置的固定值设置appendAll_, maxAppendLevelIdx_, sequenceNumTriggerMerge_;
  void mayResetParametersForMerge();

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  //返回字符串：每一层文件的个数
  const char* LevelSummary(LevelSummaryStorage* scratch) const;
  std::string DebugFullRangeString() const;

 private:
  class Builder; //调用的格式为 Builder + Apply* + SaveTo + ~Builder 最终根据current_和一个或多个VersionEdit对象生成一个新的Version对象，该Version对象设置了ranges_, ranges_中的RangeMetaData的每个成员都做了初始化;

  friend class TaskInfo;
  friend class Version;
  friend class RangeInfoForEdit;

  void GetOverlappingTasks(
      const InternalKeyComparator& icmp,
      const std::set<FullTask, BySmallest>& tasks,
      const InternalKey* begin,
      const InternalKey* end,
      std::vector<FullTask>* overlapTasks);
  
  //inputs为输入，smallest,largest为输出：inputs描述的所有文件的IternalKey的范围
  void GetRange(const std::vector<RangeMetaData*>& inputs,
                InternalKey* smallest,
                InternalKey* largest);

  // 设置inputs1，inputs2合并后的smallest InternalKey和largest InternalKey
  void GetRange2(const std::vector<RangeMetaData*>& inputs1,
                 const std::vector<RangeMetaData*>& inputs2,
                 InternalKey* smallest,
                 InternalKey* largest);

  // Save current contents to *log
  Status WriteSnapshot(log::Writer* log);
 

  int64_t WillMostNumRange(int level) { 
    if(level == maxStableLevelIndex_ + 1 )
      return current_->ranges_[level].size() + rangeAddingNum_[level] + maxPureAddingNumForInstable_;
    else 
      return current_->ranges_[level].size() + rangeAddingNum_[level];
  }     //判断range个数需增加的层使用
  int64_t WillLeastNumRange(int level) { return current_->ranges_[level].size() - rangeReducingNum_[level]; }  //判断level层将得到的最少range个数
  bool ShouldReduceRange(int level);
  bool CanAddRange(int level);
  int CanAddMostRangeNum(int level);
  //需持有S锁, smallest和largest都为InternalKey格式
  void GetImmRange(MemTable* imm, InternalKey& smallest, InternalKey& largest){
    Iterator* iter = imm->NewIterator();
    iter->SeekToFirst();
    smallest.DecodeFrom(iter->key());
    iter->SeekToLast();
    largest.DecodeFrom(iter->key());
    delete iter;
  }

  void getChildsInfo(int targetLevel, int maxDiff, \
                     std::vector<int64_t>& shiftIndex, \
                     std::vector<unsigned char>& overlapNums, \
                     std::vector<unsigned char>& combinedChildNums);

  //调用前确保 本层为最大的稳定层 或 下层没有相交的满的，或 range没有数据
  //对在索引为targetLevel的层的range构造任务返回(new得到的)，首先对range要加锁成功;然后对于shift/split任务要保证被增加的层的range个数不大于最大值,要减少的层的个数大于阈值;对于flush操作，将所有被覆盖的range也要加锁成功,对任意一条条件不满足则返回NULL;
  TaskInfo* ConstructTaskInfo(RangeMetaData * range, MemTable* imm, port::Mutex* mutexImmR, int targetLevel, bool isFlushForCombine);
  TaskInfo* getTaskForGarbage(RangeMetaData * range, int targetLevel);

  //深度遍历找出以task(在索引为level的层中)为根,与fullTask_有相交关系的为孩子的树(注意非稳定层不属于该树)的 未被加锁的第一个叶子任务返回,
  //根据不同的任务(shift/ajustment-flush/split,对于split/shift若增加个数的层的个数超出最大值则跳过)，其相关锁也被加锁成功;
  //若不存在这样的任务则返回NULL;
  TaskInfo* FindLeafTask(const FullTask& task, int level);

  //对于flush操作,设置taskinfo->isChildMerge以指导孩子进行merge还是append
  void SetIsChildMerge(TaskInfo* taskinfo);

  //idx既为输入也为输出,levelIdx为输出：根据range的编号，输出该range所在的levelIdx和在该层的idx
  bool getIdxInOneLevel(int& idx, int& levelIdx);

  void VerifyInfos();

  //将v插入到以dummy_version为头节点的双向链表中，当前版本current_ = v 等
  void AppendVersion(Version* v);

  Env* const env_;
  const std::string dbname_;
  const Options* const options_;
  MtableCache* const mtable_cache_;
  const InternalKeyComparator icmp_;
  uint64_t next_file_number_; //既可能用来设置manifest_file_number_,也可能设置log_number_
  uint64_t manifest_file_number_;//正在使用的MANIFEST文件的number
  uint64_t last_sequence_;   //最新的（或上一个）sequence number，即InternalKey的那个
  uint64_t log_number_;      //>=log_number的log文件是未被转换为(M)SStable的，正在写的log文件的number(Imm成功下移才会将该log number写入Manifest文件, 故恢复时小于该log number的都可以删除)
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted(未写入磁盘的最小的log文件的number,现在版本没有什么用)

  // Opened lazily
  WritableFile* descriptor_file_;//manifest文件相关, 为创建descriptor_log_的参数
  log::Writer* descriptor_log_; //manifest文件相关, Writer为写日志的类
  Version dummy_versions_;  // Head of circular doubly-linked list of versions.
  Version* current_;        // == dummy_versions_.prev_

//.............................
  //供fullTask_排序使用:根据最小的InternalKey排序
  BySmallest smallestCmp_;
  std::set<FullTask, BySmallest>* fullTask_[config::kNumLevels]; //指针数组,kNumLevels默认为7 ,Recover中初始化, 需要用set，因为有插入删除等操作;
  int32_t maxStableLevelIndex_;       //最大的稳定层的索引,构造函数中被初始化为-1,在Recover中被正确的初始化设置, 在pickDiskTask中会被更新和使用
  int32_t maxPureAddingNumForInstable_; //非稳定层被flush下来的数据 或 非稳定层的垃圾回收 可能也会增加range数，此range数的精确值不好获得，所以此变量为非稳定层可增加的最大值(在进行的所有线程对非稳定层可能增加的个数之和 - 删除原来的将会被删除的满的range); 对于maxStableLevelIndex_为-1，即level 1为非稳定层的情况，因为Immutable 下移只可能是单线, 且垃圾回收只对第三层的最后一层开始进行，所以maxPureAddingNumForInstable_只能为0

  int32_t rangeReducingNum_[config::kNumLevels]; //下面的构造函数中初始化
  int32_t rangeAddingNum_[config::kNumLevels];
  struct CombineToDo{
    InternalKey smallest; //记录的两个信息都可能发生变化
    double point;
    void clear() { smallest = InternalKey(); point = -1000;}
    void set(InternalKey &smallest, double point) { this->smallest = smallest; this->point = point; }
  };
  CombineToDo combineTodos_[config::kNumLevels]; //pickDiskTask函数中被设置和更新

  int numDoingGarbageCollection_; //后台线程同时在进行垃圾回收的个数

  int32_t emptyRangeNum_[config::kNumLevels]; //对于还没有数据的层初始化为0

  bool isAllShift_; //全部是shift类型的操作(顺序写场景中),若为true(初始为true)便不用进行垃圾回收的判断了

  const int pagesize_; //下面七个个变量辅助判断flush操作的处理孩子时"是否用归并替代追加操作的"
  int64_t dataAmountExcludeMeta_[config::kNumLevels]; 
  //此两个变量只在setDataInfo函数中设置setParametersForMerge使用,无需初始化
  PairFIFOPool levelCachedInfos_[config::kNumLevels];  //pair中存储的信息为<total pages num, cached pages num> for one sampled file; 对象本身多线程安全; 用默认构造函数初始化
  double totalCachedDataRatio_;

  bool appendAll_;
  int maxAppendLevelIdx_;
  int sequenceNumTriggerMerge_;

//..............................

  // No copying allowed
  VersionSet(const VersionSet&);
  void operator=(const VersionSet&);
};

// A TaskInfo encapsulates information about a task
// 只是一个task的信息,对应得迭代器在VersionSet::MakeInputIterator可在函数中生成,实际的task工作的执行在dbimpl.h/cc中进行).
class TaskInfo {
 public:
  // Return the level that is to be manage
  int level() const { return level_; }
  bool isMemTask()  const { return imm != NULL; }
  // Return the object that holds the edits to the descriptor done by this task
  VersionEdit* edit() { return &edit_; }
  // "which" must be either 0 or 1
  int num_input_files(int which) const { return inputs_[which].size(); }

  std::vector<RangeMetaData*>& inputs(int which) {
    return inputs_[which];
  }
  std::vector<RangeMetaData*>(&inputs())[2] { //返回数组
    return inputs_;
  }
  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  RangeMetaData* input(int which, int i) const { return inputs_[which][i]; }
  bool isFlushTask() { return type_ == tAdjustmentFlush || type_ == tCombineFlush; }
  //对于tCombineFlush || type_ == tCombineShift || type_ == tSplit操作，inputs_[0]的记录的range插入edit的deleted ranges集合
  //对于isFlushToInstable, inputs_[1]中满的range插入edit的deleted ranges集合
  void AddInputDeletions(VersionEdit* edit);

  //解锁本任务的加的所有R锁,若是immutable memtable的相关任务,还释放mutexImmR锁
  void UnlockRs(port::Mutex* mutexImmR){
    if(isMemTask()) {
      mutexImmR->Unlock();
    }
    for(int which = 0; which < 2; ++which) {
      for(int i = 0 ; i < inputs_[which].size(); ++i){
        inputs_[which][i]->filemeta->range_lock.Unlock();
      }
    }
  }

  enum TaskType{
    tCombineShift = 1,
    tDataShift,
    tCombineFlush,
    tAdjustmentFlush,
    tSplit,
    tGarbageCollection,
    tInvalid
  };
  TaskType tasktype() const { return type_; }

  bool isFlushToInstable;
  int immShiftLevel; //只对imm shift操作时设置
  int maxPureAddingNumForInstable; //这两个变量 "flush到非稳定层"或"最后一层垃圾回收"有意义,默认为0,用于LogAndApply恢复maxPureAddingNumForInstable_
  int maxNewedNumForInstable;        //初始化为0,DoMainDiskJob时设置

  std::string divideUserkeyForSplit;
  //暂只对flush操作有效,场景1: integratedAppendMerge为true,指示inputs_[1]的每个元素是否进行归并;对于shift,暂时无效; //有效场景2:对于append后发现文件未满,但是写时发现hole中连索引都写不上,此时需要对原文件归并(在执行磁盘操作时才设置)
  std::vector<bool> isChildMerge; 

 private:
  friend class Version;
  friend class VersionSet;

  //供友元类调用, divideUserkeyForSplit, edit_, inputs_, grandparents_调用默认构造函数
  TaskInfo(int level, TaskType type): level_(level), type_(type), imm(NULL), \
                             isFlushToInstable(false), immShiftLevel(-1), maxPureAddingNumForInstable(0), maxNewedNumForInstable(0) {
  }

  const int level_;
  VersionEdit edit_;

  const TaskType type_;
  MemTable* imm;
  // Each task reads inputs from "level_" and "level_+1", 都会被加锁
  std::vector<RangeMetaData*> inputs_[2];      // The two sets of inputs

};


}  // namespace branchdb

#endif  // STORAGE_BRANCHDB_DB_VERSION_SET_H_
