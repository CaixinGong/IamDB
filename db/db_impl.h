
#ifndef STORAGE_BRANCHDB_DB_DB_IMPL_H_
#define STORAGE_BRANCHDB_DB_DB_IMPL_H_

#include <deque>
#include <set>
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "branchdb/db.h"
#include "branchdb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace branchdb {

class MemTable;
class MtableCache;
class Version;
class VersionEdit;
class VersionSet;

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);
  virtual ~DBImpl();

  // Implementations of the DB interface
  virtual Status Put(const WriteOptions&, const Slice& key, const Slice& value);
  virtual Status Delete(const WriteOptions&, const Slice& key);
  virtual Status Write(const WriteOptions& options, WriteBatch* updates);
  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value);
  virtual Iterator* NewIterator(const ReadOptions&);
  virtual const Snapshot* GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot* snapshot);
  virtual bool GetProperty(const Slice& property, std::string* value);
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  // 供NewInteratort调用,用于读的优化
  void RecordReadSample(Slice key);

 private:
  friend class DB;//DB::open 函数可以使用本类的私有成员
  struct TaskProcedureState;
  struct Writer;

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor 已经被logAndApply
  Status Recover();

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles(bool immDownSuccess, bool keepDescriptorFile=true); //需持有S锁

  Status RecoverLogFile(uint64_t log_number,
                        SequenceNumber* max_sequence);
  Status WriteMemForRecover();

  Status MakeRoomForWrite(bool force /* compact even if there is room? */)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  WriteBatch* BuildBatchGroup(Writer** last_writer);

  void RecordBackgroundError(const Status& s);

  void CleanupTask(TaskProcedureState* taskProcedure); //须持有S锁

  Status OpenTaskOutputFile(TaskProcedureState* taskProcedure, const Slice& key, int& openedChildIdx, bool& isNewed, uint64_t& file_number, bool normalAppend2hole);
  void SetNewOpenInfos(RangeInfoForEdit& rangeEdit, uint64_t& file_number, uint64_t& offset1, uint64_t& index_offset, uint64_t& holeSize, bool isSStable);
  void SetExistOpenInfos(RangeInfoForEdit& rangeEdit, uint64_t& file_number, uint64_t& offset1, uint64_t& index_offset, uint64_t& holeSize, const RangeMetaData* existRange);
  bool ConditionForFinishFile(const TaskProcedureState* taskProcedure, const Iterator* input, int openedChildIdx, const std::string& current_user_key, const std::string& firstKeyInsert);
  Status FinishTaskOutputFile(TaskProcedureState* taskProcedure, Iterator* input, int openedChildIdx, bool isNewed, uint64_t file_number);

  port::CondVar* wakeCondForReleaseDB() {
    return &condForShutDown_;
  }
  port::Mutex* wakeMutexForReleaseDB() {
    return &mutexForShutDown_;
  }
  static void BGWork(void* db);
  void BackgroundTaskPerform();
  Status DoMainDiskJob(TaskProcedureState* taskProcedure);  //无需持有S锁
  void SetDivideKeysForFlushStable(TaskProcedureState* taskProcedure); //需持有S锁
  void SetDivideKeysForFlushInstable(TaskProcedureState* taskProcedure); //无需持有S锁
  bool IsBaseLevelForKey(const Slice& user_key); //须持有锁S
  void SetApplyinfo(TaskProcedureState* taskProcedure, ApplyInfo* applyinfo); //无需持有S锁
  //根据taskProcedure, 设置taskProcedure->taskinfo.edit_, 注意对于Imm任务需要记录LogNumber
  void setVersionEdit(TaskProcedureState* taskProcedure, bool isRecoverProcess = false);
  void setKeysForAdjust(RangeInfoForEdit& rangeEdit, TaskProcedureState* taskProcedure);  //需已经持有S锁


  // Constant after construction
  Env* const env_;
  const InternalKeyComparator internal_comparator_;//这个构造函数的参数默认使用BytewiseComparatorImpl
  const InternalFilterPolicy internal_filter_policy_;//默认为NULL
 //通过options_.comparator为 &internal_comparator_,因为Memtable插入和读出的为InternalKey 和value,在将Memtable内容已经SStable文件的内容读出并写入SStable，再痛options_.comparator使得数据库内部使用的都是InternalKey(即 user key + tag(sequence number {高7Bytes} + type {低1 Byte}) ) + value
 //而过滤器使用的都是internal_filter_policy_(或说options_.filter_policy)，所以都是将InternalKey重新解析基于userkey进行处理的
 //这两者都在SanitizeOptions函数中构造的
 //因为都是迭代器产生的是InternalKey+value，所以有DBIter用于将内部的迭代器转换为用户的迭代器用于用户接口DBImpl::NewIterator
  const Options options_;  // 注意为const类型，options_.comparator == &internal_comparator_ 默认类型为BytewiseComparatorImpl(可设置)
  bool owns_info_log_; //info_log 是由branchdb的用户通过option传递进来的的则为false，否则为true(DBimpl内部自己new的)
  bool owns_cache_;    //block cache是由branchdb的用户通过option传递进来的则为false，否则为true(DBimpl内部自己new的)
  const std::string dbname_;

  // table_cache_ provides its own synchronization
  MtableCache* mtable_cache_;

  // Lock over the persistent DB state.  Non-NULL iff successfully acquired.
  FileLock* db_lock_; //实际赋值类型为PosixFileLock*

  port::Mutex mutexImmR_;   //Immutable Memtable的R锁
  port::Mutex mutexB_;            //B(both,即logAndApply)锁
  port::Mutex mutexForShutDown_;
  port::CondVar condForShutDown_;  //用于后台唤醒析构本db的主线程
  bool shutting_down_; //用于指示数据库正在关闭，不要再向env->threadpool队列插任务
  port::Mutex mutexS_;  //待优化：使用该锁的线程过多，每个用户线程和后台线程都要使用此锁(可以尝试拆分成两种锁)
  //下面这些变量都受mutexS_的保护(除下面的writers_)
  port::CondVar condForImmDownOrErr_;          // Signalled when the immutable Memtable move down work finishes 加上一些不可继续进行的错误出现了
  //再加上一个唤醒前台线程的互斥量,该互斥量只需唤醒因Memtable和Immutable Memtable都存在而睡眠的前台线程
  //(为了简化可以将mutexForShutDown_和mutexS_以及互斥量合并,但如果不做区分那么后台线程每次完成任务后都得Signal两类线程,后台主线程和用户线程,SignalAll)

  // State below is protected by mutexS_(包括下面所有变量以及对象内部变量的读写)
  MemTable* mem_;
  MemTable* imm_;                // Memtable being compacted
  WritableFile* logfile_;        //下面三个与写log有关，相互绑定(可见open函数)
  uint64_t logfile_number_;      //当前写的log的最新number. 一旦本logfile_number_写入MANIFEST日志文件(在immutable memtable持久化的任务成功后进行),表示小于该logfile_number_的日志文件可以删除(一般immutable memtable对应的日志文件可以删除)
  log::Writer* log_;
  uint32_t seed_;                // For sampling.产生随机数的种子

  bool isDoingImmTask_; //MakeRoomForWrite函数内,用户信息使用此信息判断是否需要等待

  // Queue of writers.
  port::Mutex writers_lock_; //专门保护writes_变量
  std::deque<Writer*> writers_;
  WriteBatch* tmp_batch_;  //构造函数为其new一个，再将多个WriteBatch合并为一个插入Mem时作为临时对象

  SnapshotList snapshots_; //数据库维持的所有快照双向循环链表

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_;

  VersionSet* versions_;

  // Have we encountered a background error in paranoid mode?
  Status bg_error_;

  // Per level task stats.  stats_[level] stores the stats for
  // task that produced data for the specified "level".
  // (即使只有添加没有merge操作也算compaction,如从日志中恢复的Level0)
  struct TaskStats {
    int64_t micros; //压缩使用时间
    int64_t bytes_read; 
    int64_t bytes_written;

    TaskStats() : micros(0), bytes_read(0), bytes_written(0) { }

    void Add(const TaskStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }
  };
  TaskStats stats_[config::kNumLevels];//记录的是生成本层数据的操作的花费, level 2记录的是level 1的下移操作等, level 1记录的是imm的下移，故对于stats_[0](即生成level 1数据的操作)读的数据量为0; 但对于垃圾回收记录的是当前层的

  // No copying allowed
  DBImpl(const DBImpl&);
  void operator=(const DBImpl&);

  const Comparator* user_comparator() const {// 返回类型为BytewiseComparatorImpl* 
    return internal_comparator_.user_comparator();
  }
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
extern Options SanitizeOptions(const std::string& db,
                               const InternalKeyComparator* icmp,
                               const InternalFilterPolicy* ipolicy,
                               const Options& src);

}  // namespace branchdb

#endif  // STORAGE_BRANCHDB_DB_DB_IMPL_H_
