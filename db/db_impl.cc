// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <iostream>
#include <fcntl.h>
#include <sys/stat.h>
#include <stdlib.h>
#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/mtable_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "branchdb/db.h"
#include "branchdb/env.h"
#include "branchdb/status.h"
#include "branchdb/mtable.h"
#include "branchdb/mtable_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace branchdb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
// 因为在DBImpl中所有的writer以deque组织,以实现先提交的写先被服务
struct DBImpl::Writer {
  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;

  //初始化了封装了条件变量的类的对象cv，status调用默认构造函数
  explicit Writer(port::Mutex* mu) : cv(mu) { }
};

//DBImpl的私有嵌套类, 记录某一后台线程执行task的任务状态
struct DBImpl::TaskProcedureState {
  TaskInfo* const taskinfo;
  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  // 注释的意思是：因为一个数据库可能维持着多个快照，这个成员记录的是最老的那个快照，自然不会再有比smallest_snapshot更小的快照。故
  // 若遇到比这个快照小的sequence number的记录，且这个记录可以被删除（如已经有新值覆盖），则可以真正的从数据库删除
  SequenceNumber smallest_snapshot;//构造函数中未初始化, 在DoMainDiskJob中初始化
  // State kept for output being generated
  enum CurrentOutputType{
    newed = 1,
    modified,
    invalid
  };
  PwritableFile* outfile; //下面两个都关联到outputs中的当前的(M)SStable文件, 当写完当前的(M)SStable文件，并且同步完成，builder被置为NULL
  MtableBuilder* builder;
  uint64_t total_bytes; //此次task至今写磁盘的字节总数
  uint64_t total_bytes_exclude_meta; //此次task至今写磁盘的字节总数, 不包括元数据
  //只在flush到非稳定层时 或 非稳定层的垃圾回收时 设置,表示非稳定层操作到现在新建的range实际个数,每新建一个range就+1
  int newedNumForInstable;
  //在flush时,当插入的key落在两child之间会利用此信息判断插入哪个child，该数组的个数为child的个数-1(基于user key得到)
  std::vector<std::string> divideChildUserkeys;

  CurrentOutputType cOutputType() const {
    return currentOutputType;
  } 

  bool isRangeEditEmpty() {
    return  (modifiedOutputs.size() == 0 && newedOutputs.size() == 0);
  }

  RangeInfoForEdit* currentRangeEdit() {
    if(currentOutputType == modified)  return &modifiedOutputs[modifiedOutputs.size()-1];
    else if( currentOutputType == newed) return &newedOutputs[newedOutputs.size()-1];
    return NULL;
  }
  void pushBackOneOutput(RangeInfoForEdit& rangeEdit, CurrentOutputType outputType ) {
    currentOutputType = outputType;
    if(outputType == modified) modifiedOutputs.push_back(rangeEdit);
    else if(outputType == newed) newedOutputs.push_back(rangeEdit);
    else assert(0);
  }
  void PopBackOneOutput() { //finish时因为空间不够时而finish失败，需要重新插入时调用
    if(currentOutputType == modified) {
      assert(modifiedOutputs.size() >=1);
      modifiedOutputs.pop_back();
     }
    else if( currentOutputType == newed) {
      assert(newedOutputs.size() >=1);
      newedOutputs.pop_back();
    }
    currentOutputType = invalid; //之前push_back进行来的的是不能改变的,也不(必)知道哪一个谁最后push_back进来的
  }
  const std::vector<RangeInfoForEdit>* getModifiedOutputs() {
   return &modifiedOutputs;
  }
  const std::vector<RangeInfoForEdit>* getNewedOutputs() {
   return &newedOutputs;
  }

  explicit TaskProcedureState(TaskInfo* t)
      : taskinfo(t),  //smallest_snapshot未初始化, 在DoMainDiskJob中初始化
        //两种outputs 和 optStableChildLimit调用默认构造函数, 都在DoMainDiskJob中设置和初始化
        currentOutputType(invalid),
        outfile(NULL),
        builder(NULL),
        total_bytes(0),
        total_bytes_exclude_meta(0),
        newedNumForInstable(0) {
  }
  //用于减少设置范围时使得一个range属于两个父亲的孩子的情况, 意义为std::pair<std::pair<largest limit for childs[idx], smallest limit for childs[idx+1]> > (empty string means no limit), elements num == childs num-1;  Only for flush to stable level.
  std::vector<std::pair<std::string, std::string> > optStableChildLimit;

 private:
  // ranges produced by this task, 只记录在DoMainDiskJob中进行磁盘操作而被修改获得的或新生成的range,其中待删除的在taskinfo中的input[2]数组中记录(根据不同的任务可以提取出被删除range信息),这里不记录, 未进行磁盘修改操作的range也不记录,如data-shift或adjustment-flush的父亲节点; 
  // 一个例外:磁盘range的shift操作(combine-shift/data-shift)，无需磁盘操作(Imm的combine-flush除外)但新的range会插入newedOutputs中;
  std::vector<RangeInfoForEdit> modifiedOutputs;
  std::vector<RangeInfoForEdit> newedOutputs;
  CurrentOutputType currentOutputType;
};




// Fix user-supplied options to be reasonable
// 若*ptr的值太小或太大，则将*ptr的值修正为minvalue或maxvalue
template <class T,class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
//Sanitize(清除...的不健康成分）: 返回根据传入的src 设置Options对象result,包括：
//修改comparator, filter_policy, max_open_files, write_buffer_size, block_size, info_log, block_cache 其他的不变
//主要修改了 info_log, block_cache ,其他的相对于src一般不变。
//返回的result的，对于多线程程序,其中的env指向的对象PosixEnv是共享的(但是是线程安全的）,其他的每个线程不共享
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src; //使用合成的赋值构造函数
  result.comparator = icmp; //注意两种类型不一样
  result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;//注意两种类型不一样
  ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000); //默认为1000, [74,50000]之间
  ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30); //默认为4MB   [64KB, 1G]之间
  ClipToRange(&result.block_size,        1<<10,                       4<<20); //默认为4KB   [1KB, 4MB]之间
  if (result.info_log == NULL) { //若用户没有设置的，这里用默认的(Option构造函数默认为NULL ,这里生成logger* ,赋值给result)
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // In case it does not exist(存在将创建失败)
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));//将LOG文件名命名为LOG.old文件
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);//打开传入的文件名(不存在则创建)以创建PosixLogger对象，
                                                                             //赋值给result.info_log
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = NULL;
    }
  }
  if (result.block_cache == NULL) { //若用户没有设置的，这里用默认的
//    result.block_cache = NewLRUCache(8 << 20); //block_cache的默认大小总的为8MB(一个data block的默认大小为4KB) 故可以放下约2048个
//    result.block_cache = NewLRUCache(64 << 20); //block_cache的默认大小总的为8*8MB(一个data block的默认大小为4KB) 故可以放下约2048*8个
    result.block_cache = NewLRUCache(256 << 20); //block_cache的默认大小总的为32*8MB(一个data block的默认大小为4KB) 故可以放下约2048*32个
  }
  return result;
}

//ok
DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator), //这个构造函数的参数默认使用BytewiseComparatorImpl(可设置)
      internal_filter_policy_(raw_options.filter_policy),//参数默认为NULL(可设置),注意这里用的是InternalFilterPolicy,加了一层,对user key处理
      //修改comparator, filter_policy, max_open_files, write_buffer_size, block_size, info_log, block_cache 其他的不变
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),// 对于多线程程序,options_->env指向的对象PosixEnv是共享的,其他不共享
      owns_info_log_(options_.info_log != raw_options.info_log),   //info_log 是由branchdb的用户new的则为false，否则为true
      owns_cache_(options_.block_cache != raw_options.block_cache),//block cache是由branchdb的用户new的则为false，否则为true
      dbname_(dbname),
      db_lock_(NULL),
      //mutexImmR_, mutexB_, mutexForShutDown_在此调用了默认构造函数
      condForShutDown_(&mutexForShutDown_),
      shutting_down_(false),
      //mutexS_在此调用构造函数
      condForImmDownOrErr_(&mutexS_),
      mem_(new MemTable(internal_comparator_)),
      imm_(NULL),
      logfile_(NULL),
      logfile_number_(0),
      log_(NULL),
      seed_(0),
      isDoingImmTask_(false),
      //writers_lock_, writers_在此调用默认构造函数:空队列
      tmp_batch_(new WriteBatch)
      //snapshots_ pending_outputs_在此调用默认构造函数
      /* bg_err_ stats_在此调用默认构造函数*/ { // 未初始化的versions_ table_cache_在函数体初始化

  mem_->Ref();

  // Reserve ten files or so for other uses and give the rest to TableCache.
  const int mtable_cache_size = options_.max_open_files - kNumNonTableCacheFiles; //默认为1000-10为990
  mtable_cache_ = new MtableCache(dbname_, &options_, mtable_cache_size);

  versions_ = new VersionSet(dbname_, &options_, mtable_cache_,
                             &internal_comparator_);
}

//将shutting_down_值为ture，等待所有后台任务结束，释放对LOCK文件的独占性写锁，回收new过的内存资源
//ok
DBImpl::~DBImpl() {
  // Wait for background work to finish
  mutexForShutDown_.Lock(); //保护shutting_down_,其置位后,关于本db的所有任务移除后不能再将本任务插入队列(由DBImpl控制)
  shutting_down_ = true;
  //通知env的后台线程池队列中将所使用的db全部从队列中移除
  env_->RemoveAllDBTask(BGWork, this);
  //判断是否有env执行关于本db的线程, 若有则wait并等待env关于db的任务执行完后signal本线程(在env->ThreadPool中实现),醒来后继续判断是否有env执行关于本db的线程
  while(env_->isDoingForDB(BGWork, this)) {
    condForShutDown_.Wait();
  }
  mutexForShutDown_.Unlock();

//后面可以保证后台线程不会再访问本db的信息,所以无需加锁
  if (db_lock_ != NULL) {
    env_->UnlockFile(db_lock_);//释放对LOCK文件的独占性写锁
  }

  delete versions_;
  if (mem_ != NULL) mem_->Unref();
  if (imm_ != NULL) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete mtable_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }  if (owns_cache_) {
    delete options_.block_cache;
  }
}

//创建名为sunLevel/MANIFEST-000001的文件(格式为log),写入一条将VersionEdit对象(函数开头简单的初始化了3个计数器,log_number_(0), next_file_number_(2), last_sequence_(0) )序列化的后的内容作为value日志
//向sunLevel/CURRENT文件中 写入当前的MANIFEST文件名即"MANIFEST-00001\n", 确保刷入磁盘，关闭文件。
//ok
Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1); //初始的manifest文件名为sunLevel/MANIFEST-000001
  WritableFile* file;
  Status s = env_->NewWritableFile(manifest, &file); //打开manifest文件即sunLevel/MANIFEST-000001作为参数以new PosixWritableFile对象写入file
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file); //用关联了manifeset的文件的PosixWritableFile对象创建一个写日志的对象
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record); //将上面的VersionEdit对象 new_db序列化作为一条记录写入manifest文件
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {//当manifest文件成功添加一条记录
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);//向sunLevel/CURRENT文件（不存在将新建,存在原来的被删除）中 写入当前的MANIFEST文件名即"MANIFEST-00001\n", 确保CURRENT>刷入磁盘，关闭文件。
  } else {
    env_->DeleteFile(manifest); //即unlink
  }
  return s;
}

//如果options_.paranoid_checks被设置，若s状态no ok，将保留s的状态，还是no ok(后面可能会终止程序)
//若为false，则写一条信息给LOG文件，将s状态设置为ok
//当让只能对于可忽略的错误使用，这里只在恢复用户日志时使用,如用户日志的部分数据损坏等
void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

//须持有S锁
//删除多余的文件，包括log(只有是Imm下移的线程才能删除)，MANIFEST，SSTable(.ldb), kTempFile(.dbtemp)。LOG LOG.old CURRENT LOCK不删除
//pending_outputs_和versions_的每个版本有记录的文件保留(构成live集合) 不在live集合中kTableFile和kTempFile将被删除
//若是SSTable文件需要删除，还要从TableCache中显示驱除
//无法识别的文件 不 删除
void DBImpl::DeleteObsoleteFiles(bool immDownSuccess, bool keepDescriptorFile) {
  if (!bg_error_.ok()) { //初始时为ok
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // Make a set of all of the live files(live集合对kTableFile和kTempFile[pending_outputs_中记录]有用)
  // 不在live集合中kTableFile和kTempFile将被删除
  std::set<uint64_t> live = pending_outputs_;
  versions_->AddLiveFiles(&live, true);  //percent==0也得添加进去: 因为其他线程可能正在新建该文件,新建出来就被本线程给删除了


  std::vector<std::string> filenames;
  env_->GetChildren(dbname_, &filenames); // Ignoring errors on purpose ,获得dbname_下的所有文件名至filenames
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          keep = ( !immDownSuccess ||       //不是imm成功下移则保留所有的log文件 即 不是Imm任务或Imm下移不成功则需保留Imm对应的log文件
                  (number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile: //即MANIFEST文件
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          keep = keepDescriptorFile || (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {
        if (type == kTableFile) { //若是需要删除的SSTable文件，需从Table Cache中驱除
          mtable_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n",
            int(type),
            static_cast<unsigned long long>(number));
        env_->DeleteFile(dbname_ + "/" + filenames[i]);//unlink需要删除的文件
      }
    }
  }
}

//私有函数
//若本次为新建数据库：
    //创建数据库目录(已经在DBImpl::SanitizeOptions中创建);创建MANIFEST文件,number为1并写入一条内容十分简单的序列化的VersionEdit,并同步写CURRENT，然后调用VersionSet::Recover获得VersionSet的成员的初始设置
//若本次为以前已经存在的数据库：
    //调用VersionSet::Recover获得VersionSet在上次数据库关闭前持久化到磁盘的设置,然后,若有log文件未写入SStable,则将每个log文件其转化为一个或多个(若log文件大于4MB，则分为多个4MB，最后一份除外)转换为Imm_并调用BackgroundTaskPerform进行操作以将Imm_写入磁盘并LogAndApply操作,并设置VersionSet的next_file_number_，max_sequence
Status DBImpl::Recover() {
  mutexS_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);//创建目录SunLevel，用户:读写执行; 组: 读 执行; 其他: 读 执行,若存在目录创建不成功,保留以前的
  assert(db_lock_ == NULL);
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);//LOCK的整个文件如sunLevel/LOCK加独占性写锁(对进程间同步有效),并将该文件名插入env_->locks_中(记录了加锁的文件名而已),返回db_lock_(记录了加锁的fd，name而已)
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {//CURRENT文件不存在, 即数据库以前未被创建
    if (options_.create_if_missing) {
      //创建名为sunLevel/MANIFEST-000001的文件(格式为log),写入一条将VersionEdit对象(函数开头简单的初始化了3个计数器,log_number_(0), next_file_number_(2), last_sequence_(0), 和比较函数的名字)序列化的后的内容以日志格式写入MANIFEST-000001文件
      //向sunLevel/CURRENT文件中 写入当前的MANIFEST文件名即"MANIFEST-00001\n", 确保CURRENT文件刷入磁盘(因为下面要读)，关闭文件。
      s = NewDB();
      if (!s.ok()) {
        return s;
      }
    } else {
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else { //CURRENT文件存在,即数据库以前已经被创建(也就是说CUREENT文件，MANIFEST文件都存在)
    if (options_.error_if_exists) {
      return Status::InvalidArgument(
          dbname_, "exists (error_if_exists is true)");
    }
  }

//Recover()函数:读CURRENT指向的MANIFEST文件的所有记录(VersionEdit的序列化)进行综合，构造出一个version，作为当前版本(插入版本的双向链表中),并更新全部计数值(5个计数值 初始为next_file_number_(2), manifest_file_number_(0), last_sequence_(0),  log_number_(0),  prev_log_number_(0)):
   //根据记录的最后一个edit记录的值做改变（manifest_file_number_为next_file_number,若刚创建这里将为2, next_file_number要+1),若最后一个edit的对应的计数值未设置,根据上一条edit的值设置，以此类推，都没有为0。
  s = versions_->Recover();

  if (s.ok()) {
    SequenceNumber max_sequence(0); //caixin: typedef uint64_t SequenceNumber;

    // Recover from all newer log files than the ones named in the
    // descriptor即MANIFEST文件 (new log files may have been added by the previous
    // incarnation[肉体,实体] without registering them in the descriptor).
    //
    // Note that PrevLogNumber() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of branchdb.
    const uint64_t min_log = versions_->LogNumber(); //若数据库以前不存在，这两个值为0, 0;若存在，可以为如3, 0  此参数在Imm下移成功后才会记录此log number, 故小于该log number的都可以删除
    const uint64_t prev_log = versions_->PrevLogNumber(); //只为兼容性
    std::vector<std::string> filenames;
    s = env_->GetChildren(dbname_, &filenames);//将dbname_目录下的每一个文件名push_back至filenames
    if (!s.ok()) {
      return s;
    }
    std::set<uint64_t> expected;
    versions_->AddLiveFiles(&expected, false);//所有version的所有SSTable文件number加入expected(一个set), percent 为0的不加进来否则会不发现不存在而错误
    uint64_t number;
    FileType type;
    std::vector<uint64_t> logs;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type)) {
        expected.erase(number); //存在，则从expected檫除，剩下的是应该存在但是实际不存在的（如被用户删除了）
        if (type == kLogFile && ((number >= min_log) || (number == prev_log)))//注意是大于等于(后面半个没有用，为了与以前版本兼容), imm对应的任务成功执行后才会更新log_number_，故Imm在数据关闭之前还存在那么其log number >= min_log
          logs.push_back(number); //logs存储没有写入到SStable的log文件的number(见上面第一段注释)
      }
    }
    if (!expected.empty()) {
      char buf[50];
      snprintf(buf, sizeof(buf), "%d missing files; e.g.",
               static_cast<int>(expected.size()));
      return Status::Corruption(buf, MtableFileName(dbname_, *(expected.begin())));
    }

    // Recover in the order in which the logs were generated
    std::sort(logs.begin(), logs.end());
    for (size_t i = 0; i < logs.size(); i++) { //恢复每一个log文件
      s = RecoverLogFile(logs[i], &max_sequence);//写了SStable文件(写入磁盘，同步写函数BuildTable不加锁，以加快多线程的速度）
                    //会设置 edit(将创建的SStable(s)的metadata加入edit的level 0 的new_files集合中) 和max_sequence(本log的最大sequence number)

      // The previous incarnation may not have written any MANIFEST
      // records after allocating this log number.  So we manually
      // update the file number allocation counter in VersionSet.
      versions_->MarkFileNumberUsed(logs[i]); //为了版本兼容：根据log文件的number可能更新next_file_number_
    }

    if (s.ok()) {
      if (versions_->LastSequence() < max_sequence) {
        versions_->SetLastSequence(max_sequence);
      }
    }
    //将descriptor_log_和descriptor_file_置为NULL(若原来没有log文件则原来就为NULL)以使得下次进行LogAndApply的过程中打开新的MANIFEST文件写入快照
    versions_->SetDescriptorLogNULL();
  }

  return s;
}

Status DBImpl::RecoverLogFile(uint64_t log_number,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter { //嵌套在函数里的类
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // NULL if options_.paranoid_checks==false
    virtual void Corruption(size_t bytes, const Status& s) {
      Log(info_log, "%s%s: dropping %d bytes; %s",  //info_log为PosixLogger对象指针,向LOG文件写一条记录
          (this->status == NULL ? "(ignoring error) " : ""),
          fname, static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != NULL && this->status->ok()) *this->status = s;//若LogRepoter::status为ok,LogReporter:: status = s
    }
  };

  mutexS_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file); //将文件fname打开，并new PosixSequentialFile对象赋值给SequentialFile返回
  if (!status.ok()) {
    MaybeIgnoreError(&status);//如果options_.paranoid_checks为true(默认false)，若s状态no ok，将保留s的状态，还是no ok
                              //若为false，则写一条信息给LOG文件，然后将s状态设置为ok
    return status;
  }

  // Create the log reader.(log中有格式错误才用)
  LogReporter reporter;
  reporter.env = env_; //同一进程的所有线程共享同一PosixEnv的对象
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : NULL);
  // We intentially make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true/*checksum*/,
                     0/*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long) log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch; //默认构造函数，初始12字节全为0,格式见write_batch.cc文件开始
  MemTable* mem = NULL;
  while (reader.ReadRecord(&record, &scratch) &&
         status.ok()) {
    if (record.size() < 12) { //通过后面的SetContens函数可知该log中的value应该write_batch.cc文件开始描述的一致
      reporter.Corruption(
          record.size(), Status::Corruption("log record too small"));//同时设置
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record); //将batch的内容设置为record的内容

    if (mem == NULL) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);//遍历batch中的所有记录以写入mem,其sequence Number从batch中保存的开始，每一条记录+1)
    MaybeIgnoreError(&status);//可能一条log中的格式有误：如果options_.paranoid_checks为true(默认false)，若status状态no ok，将保留status的状态，还是no ok
                              //若为false，则写一条信息给LOG文件，然后将s状态设置为ok
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq =
        WriteBatchInternal::Sequence(&batch) +
        WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq; //设置*max_sequence，作为本函数输出之一
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) { //默认4MB
      imm_ = mem;
      mem = NULL;
      while( imm_ != NULL && status.ok()) {
        mutexS_.Unlock();
        //用于open数据库时恢复log时调用，此时Imm_已经被设置为从log文件中读出的即要恢复的内容，未持有S锁
        status = WriteMemForRecover(); //成功将Imm_下移后其会被置为NULL以及解引用
        mutexS_.Lock();
      }
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  if (status.ok() && mem != NULL) { //读完本log文件时，最后填充的一个MemTable 有内容且未满，执行
    imm_ = mem;
    mem = NULL;
    while( imm_ != NULL && status.ok()) {
      mutexS_.Unlock();
      //用于open数据库时恢复log时调用，此时Imm_已经被设置为从log文件中读出的即要恢复的内容，未持有S锁
      status = WriteMemForRecover();
      mutexS_.Lock();
    }
    // Reflect errors immediately so that conditions like full
    // file-systems cause the DB::Open() to fail.
  }

  delete file; //会关闭对应的文件
  return status;
}

//用于open数据库时恢复log时调用，此时Imm_已经被设置为从log文件中读出的即要恢复的内容，未持有S锁
//为应对"想不进行LogAndApply而将所有的log恢复似乎很难,因为很可能读取不到最新的版本信息而使得任务异常", 策略：仍进行LogAndApply操作，不过将Log的信息写进原MANIFEST文件,此时需要改变MANIFEST文件打开的类，从fopen的"w"改成"w+"? 为LogAndApply函数增加这个功能
Status DBImpl::WriteMemForRecover() {
  assert(imm_ != NULL);
  Status status;

  //step1
  mutexS_.Lock();//持有mutexS_,保护共享变量
  TaskInfo* taskinfo = versions_->PickTask(imm_, &mutexImmR_);
  mutexS_.Unlock();

  assert(taskinfo != NULL); //获取任务失败
  //这里不把任务给后台线程使得多线程执行(暂不支持)

  //step2
  //在CleanupTask函数中delete taskProcedure 
  TaskProcedureState* taskProcedure = new TaskProcedureState(taskinfo);
  //任务类型：1. 处理的是Immutable Memtable 下移任务, combine-shift类型或combine-flush类型，对于combine-flush还得判断下一层是否为非为稳定层
  //          2. 处理的flush任务，注意下一层是否为非稳定层
  //          3. 处理的是split任务
  //          4. 处理的是磁盘range的shift(combine/data-shift)任务，无需磁盘操作(Imm的combine-flush除外),将input[0]_插入新建的output队列中, 直接返回
  //操作后的信息记录在taskProcedure中, 这里记录"孩子的"modified/newed range, deleted range以及父节点无需记录其可以根据任务类型和input_[2]推断出来,具体见AddInputDeletions函数
  status = DoMainDiskJob(taskProcedure);

  //step3

  //获得taskProcedure->taskinfo.edit_和applyinfo信息用于Apply
  BySmallest bysmallest(internal_comparator_);
  ApplyInfo applyinfo(bysmallest);
  if (status.ok()) {
    SetApplyinfo(taskProcedure, &applyinfo);//根据taskProcedure(taskProcedure在DoMainDiskJob中进行设置或无需进行DoMainDiskJob)设置applyinfo 
    setVersionEdit(taskProcedure, true); //根据taskProcedure, 设置taskProcedure->taskinfo.edit_,因为imm是从log中恢复的故不设置log_number_,这样若恢复时异常退出(如断电),会重新将所有的用户log日志进行恢复
  }
  mutexB_.Lock();
  mutexS_.Lock();

  if(status.ok()) {
    VersionEdit* versionEdits[1];
    versionEdits[0] = taskProcedure->taskinfo->edit();
    TaskInfo* taskinfos[1];
    taskinfos[0] = taskinfo;
    status = versions_->LogAndApply(versionEdits, &applyinfo, 1, &mutexS_, taskinfos, &mutexImmR_, true); //会打开原来的MANIFEST文件append写
  }

  //step4
  if(status.ok() && taskinfo->isMemTask() ) {
    imm_->Unref();
    imm_ = NULL;
  }

  //将task中新建文件时分配的file number从pending_outputs_移除(这样若没有成功即没有Apply操作，产生的文件将在DeleteObsoleteFiles被删除)  
  CleanupTask(taskProcedure);
  delete taskProcedure;
  //task成功则删除所有版本中不会需要的文件;task失败删除task进行过程中的文件, 这里不删除用户log文件
  DeleteObsoleteFiles(false, true);

  mutexS_.Unlock();
  mutexB_.Unlock();
//  taskinfo->UnlockRs(&mutexImmR_);

  delete taskinfo;
  return status;

}



//将s保存到bg_error_中, 并调用condForImmDownOrErr_.SignalAll();
//ok
void DBImpl::RecordBackgroundError(const Status& s) {
  mutexS_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    condForImmDownOrErr_.SignalAll();
  }
}


//本函数和参数push_back给了后台线程的任务队列调用
void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundTaskPerform();
}

void DBImpl::BackgroundTaskPerform() {

  Status status;

  //step1
  mutexS_.Lock();//持有mutexS_,保护共享变量
  if (!bg_error_.ok()) {         //若库在执行过程中遇到不可忽略的错误
    condForImmDownOrErr_.SignalAll(); //与mutexS_绑定的互斥量，叫醒所有的用户线程
    mutexS_.Unlock();
    return;
  }
  TaskInfo* taskinfo = versions_->PickTask(imm_, &mutexImmR_);
  if(taskinfo != NULL && taskinfo->isMemTask()) { 
    isDoingImmTask_ = true;
  }
  mutexS_.Unlock();

  mutexForShutDown_.Lock(); //保护shutting_down_, 防止主线程这个时候关闭数据库且将所有队列中的任务移出,这里防止误判为没有关闭而继续将任务入队列
  //获取任务成功 且 数据库没有正在关闭 则继续放入任务;
  if( (taskinfo != NULL ) && !shutting_down_ )
    env_->Enq_task(BGWork, this, wakeMutexForReleaseDB(), wakeCondForReleaseDB() );
  mutexForShutDown_.Unlock();

  if(taskinfo == NULL) { //获取任务失败
    return;  //返回会使得后台线程的本次任务结束,继续下一次任务(若该线程池正在关闭,则执行的线程会结束,否则从队列中选取任务)
  }
  //获取任务成功(不管数据库是否正在关闭,都将该任务执行完毕)

  //step2
  TaskProcedureState* taskProcedure = new TaskProcedureState(taskinfo);  //(在CleanupTask函数中delete taskProcedure)
  //任务类型：1. 处理的是Immutable Memtable 下移任务, combine-shift类型或combine-flush类型，对于combine-flush还得判断下一层是否为非为稳定层
  //          2. 处理的flush任务，注意下一层是否为非稳定层
  //          3. 处理的是split任务
  //          4. 处理的是磁盘range的shift(combine/data-shift)任务,无需磁盘操作(Imm的combine-flush除外),将input[0]_插入新建的output队列中,直接返回
  //操作后的信息记录在taskProcedure中, 这里记录"孩子的"modified/newed range, deleted range以及父节点无需记录其可以根据任务类型和input_[2]推断出来,具体见AddInputDeletions函数
  status = DoMainDiskJob(taskProcedure);

  //step3
  //获得taskProcedure->taskinfo.edit_和applyinfo信息用于Apply
  BySmallest bysmallest(internal_comparator_);
  ApplyInfo applyinfo(bysmallest);
  if (status.ok()) {
    SetApplyinfo(taskProcedure, &applyinfo);//根据taskProcedure(taskProcedure在DoMainDiskJob中进行设置或无需进行DoMainDiskJob)设置applyinfo 
    setVersionEdit(taskProcedure); //根据taskProcedure, 设置taskProcedure->taskinfo.edit_, 注意对于Imm任务需要记录LogNumber
  }

  mutexB_.Lock();
  mutexS_.Lock();
  if(status.ok()) {
    VersionEdit* versionEdits[1];  //...待优化.....暂未采用队列以使得可能多个一起LogAndApply(队列通过锁B保护)
    versionEdits[0] = taskProcedure->taskinfo->edit();
    TaskInfo* taskinfos[1];
    taskinfos[0] = taskinfo;
    status = versions_->LogAndApply(versionEdits, &applyinfo, 1, &mutexS_, taskinfos, &mutexImmR_, false);
  }
  mutexB_.Unlock();

  //step4
  bool immDownSuccess = false;
  if(status.ok() && taskinfo->isMemTask() ) { //此过程其实属于上述Apply过程的一部分,Imm_的R锁已经释放,这里通过S锁保护Imm_
    imm_->Unref();
    imm_ = NULL;
    immDownSuccess = true; //只为下面删除多余文件时提供信息
    isDoingImmTask_ = false;
    condForImmDownOrErr_.Signal(); //叫醒可能因为mem满且Imm存在而睡眠的用户线程
  }
  else if (!status.ok() ) {//若出现错误
    RecordBackgroundError(status); //将status保存到bg_error_中(使其他执行"后面"本函数即BackgroundTaskPerform的后台线程退出), 并调用condForImmDownOrErr_.SignalAll()唤醒睡眠的所有用户线程
    if(taskinfo != NULL)
      taskinfo->UnlockRs(&mutexImmR_);
  }

  //抽样缓存信息和设置指导"是否在flush中进行归并"的参数
  bool suggestIntegrateMerge = versions_->setDataInfo(mutexS_);
  versions_->setParametersForMerge(config::instableRangeSplitNum, suggestIntegrateMerge);
  versions_->mayResetParametersForMerge();


  if(status.ok()) {
    mutexForShutDown_.Lock(); //保护shutting_down_,防止主线程这个时候关闭数据库而且将所有队列中的任务移出,而这里误判为没有关闭而继续将任务入队列
    //任务成功完成 且 数据库没有正在关闭 则继续放入任务;
    if( !shutting_down_ )
      env_->Enq_task(BGWork, this, wakeMutexForReleaseDB(), wakeCondForReleaseDB() );
    mutexForShutDown_.Unlock();
  }

  //在这输出数据库的range元信息，以及满的range的信息用于调试
  #ifndef NDEBUG 
  if(status.ok()) { 
    Log(options_.info_log,  "\n%s", versions_->current()->DebugString().c_str());
    Log(options_.info_log,  "\n%s", versions_->DebugFullRangeString().c_str());
  }
  #endif

  //将task中新建文件时分配的file number从pending_outputs_移除(这样若没有成功即没有Apply操作，产生的文件将在DeleteObsoleteFiles被删除)  
  CleanupTask(taskProcedure);
  delete taskProcedure;
  //task成功则删除所有版本中不会需要的文件;task失败删除task进行过程中的文件
  DeleteObsoleteFiles(immDownSuccess); //.....暂未根据taskProcedure记录好的要删除的包括log file进行删除(这样可以减少S锁的持有时间)

  mutexS_.Unlock();

  delete  taskinfo;
}

//须持有S锁
//将task中新建文件时分配的file number从pending_outputs_移除(这样若没有成功即没有Apply操作，产生的文件将在DeleteObsoleteFiles被删除)  
void DBImpl::CleanupTask(TaskProcedureState* taskProcedure) {
  assert(taskProcedure->outfile == NULL && taskProcedure->builder == NULL);
  const std::vector<RangeInfoForEdit>* newedOutputs =  taskProcedure->getNewedOutputs();
  for (size_t i = 0; i < newedOutputs->size(); i++) {
    const RangeInfoForEdit& newedOut = (*newedOutputs)[i];
    assert(newedOut.Get_file_number() > 0);
    pending_outputs_.erase(newedOut.Get_file_number());
  }
}


//针对除磁盘上的shift(data-shift/combine-shift)任务外的所有任务
//参数 输入key为下一个要插入的InternalKey的编码; normalAppend2hole为ture表示记录正常追加到文件洞中(否则,追加到文件的末尾)
//返回值 isNewed为true表示打开的MSStable是新建的，为false表示打开的是原来存在的MSStable(taskProcedure->currentOutputType已说明);
      // file_number用于输出此次打开的文件的编号用于在FinishTaskOutputFile函数时正确关闭文件后再打开以放入cache中;
//输入输出 openedChildIdx,只对flush有效,具体含义见函数内注释
// 结果有三种:新建SStable(只对Imm的shift);新建MSStable(flush到非稳定层需要重写的和split两种操作);打开原来的MSStable(flush):
  //首先根据任务类型判断是modified还是newed,对于newed需要获得file_number并插入pending_outputs_;
  //构建RangeInfoForEdit,若是modified设置其smallest,若是newed设置其file_number
  //打开原来的或新的range文件, 更新初始化taskProcedure.outfile,builder,对于flush到非稳定层而新增range则taskProcedure->newedNumForInstable++
Status DBImpl::OpenTaskOutputFile(TaskProcedureState* taskProcedure, const Slice& key, int& openedChildIdx, bool& isNewed, uint64_t& file_number, bool normalAppend2hole) {
  assert(taskProcedure != NULL && taskProcedure->builder == NULL);
  uint64_t offset1, index_offset, holeSize;
  RangeInfoForEdit rangeEdit;
  TaskInfo* taskinfo = taskProcedure->taskinfo;
  TaskInfo::TaskType tasktype = taskinfo->tasktype();
  if(tasktype == TaskInfo::tCombineShift && taskinfo->isMemTask()) { //imm的combine-shift操作
    //分配一个新的file_number通过参数返回，并将该file_number插入pending_outputs_进行保护;
    //设置rangeEdit的file_number; 并返回offset1, index_offset, holeSize; 其中设置的参数用于生成SSTable
    SetNewOpenInfos(rangeEdit, file_number, offset1, index_offset, holeSize, true);
    taskProcedure->pushBackOneOutput(rangeEdit, TaskProcedureState::newed); //插入新建的output队列中
    isNewed = true;
  }
  else if( (tasktype == TaskInfo::tCombineFlush || tasktype == TaskInfo::tAdjustmentFlush) ) { //flush操作: flush到非稳定层 或 稳定层
    //寻找其中要打开的(modified)或要删除的(该range满，只发生在flush到非稳定层中)openedChildIdx,(对于modified range结束条件是下一个要插入的key离下一个未满的孩子的近或落入或大于下一个未满的孩子,对于新建的结束条件除上面描述外还有: 若插入的满一半结束)
      //若找第一个child其largest>=key的range不满，将进行打开(寻找最近的range在"结束一个文件的条件判断"中进行,这里不判断而且可能可以少打开文件, 虽然可能使flush没有完全按照设置好的divided_key分配记录)
      //若找第一个child其largest>=key的range满，将进行替换, openedChildIdx可能没发生变化,但不重要因为对于满的孩子都会新建而不是打开一个MSStable, 注意再插入过程中也可能过时即可能已经遍历到了下一个"满的"孩子了
      //若不存在则使得openedChildIdx为最后一个孩子;
    std::vector<RangeMetaData*>& childs = taskinfo->inputs(1);
    if(openedChildIdx == -1) ++openedChildIdx;

    while(openedChildIdx < childs.size() && internal_comparator_.Compare(childs[openedChildIdx]->largest.Encode(), key) < 0)
      ++openedChildIdx;
    if (openedChildIdx == childs.size()) --openedChildIdx;

    if(childs[openedChildIdx]->isFull() || taskinfo->isChildMerge[openedChildIdx] ) {
      assert((taskinfo->isMemTask() || taskinfo->input(0,0)->usePercent>0));
      assert(!taskinfo->input(1,openedChildIdx)->isFull() || (taskinfo->input(1,openedChildIdx)->isFull() && taskinfo->isFlushToInstable)); //孩子满的一定是flush到最后一层的任务
      //分配一个新的file_number通过参数返回，并将该file_number插入pending_outputs_进行保护;
      //设置rangeEdit的file_number; 并返回offset1, index_offset, holeSize; 其中设置的参数用于生成MSStable
      SetNewOpenInfos(rangeEdit, file_number, offset1, index_offset, holeSize, false);
      taskProcedure->pushBackOneOutput(rangeEdit, TaskProcedureState::newed); //插入新建的output队列中
      isNewed = true;
      if(childs[openedChildIdx]->isFull()) { //暂时对于taskinfo->isFlushToInstable为true的情况下,不满的孩子即使采取了merge也不拆分，这样简单一些(虽然可能牺牲吞吐量但是可能更稳定一些)
        ++taskProcedure->newedNumForInstable;
        assert(taskProcedure->newedNumForInstable <= taskinfo->maxNewedNumForInstable);
      }
    } else {
      //将rangeEdit.smallest设置为existRange的smallest;
      //将existRange的file_number(该file_number指示的文件无需保护,因为其在当前版本中必定存在不会被删除),offset1, index_offset(offset2), holeSize返回
      SetExistOpenInfos(rangeEdit, file_number, offset1, index_offset, holeSize, childs[openedChildIdx] );
      taskProcedure->pushBackOneOutput(rangeEdit, TaskProcedureState::modified); //插入修改的output队列中
      isNewed = false;
    }
  }
  else if( tasktype == TaskInfo::tSplit) { //分裂操作
    SetNewOpenInfos(rangeEdit, file_number, offset1, index_offset, holeSize, false);
    taskProcedure->pushBackOneOutput(rangeEdit, TaskProcedureState::newed); //插入新建的output队列中
    isNewed = true;
  }
  else if( tasktype == TaskInfo::tGarbageCollection) {
    SetNewOpenInfos(rangeEdit, file_number, offset1, index_offset, holeSize, false);
    taskProcedure->pushBackOneOutput(rangeEdit, TaskProcedureState::newed); //插入新建的output队列中
    isNewed = true;
    taskProcedure->newedNumForInstable++;
    assert(taskProcedure->newedNumForInstable <= taskinfo->maxNewedNumForInstable);
  }
  else {
    assert(0);
  }

  if(!normalAppend2hole) { //若上一次尝试插入时发现空间不够只能插入索引, 则使得offset1指向末尾
    offset1 = config::fileCommonSize;
  }

  //打开MSStable, 若原文件不存在则新建，若存在则打开原文件
  std::string fname = MtableFileName(dbname_, file_number);
  Status s = env_->NewPwritableFile(fname, &taskProcedure->outfile);
  if (s.ok()) {
    taskProcedure->builder = new MtableBuilder(options_, taskProcedure->outfile, offset1, index_offset, holeSize, s);
  }

  return s;
}

//分配一个新的file_number通过参数返回，并将该file_number插入pending_outputs_进行保护;
//设置rangeEdit的file_number; 并返回offset1, index_offset, holeSize;
void DBImpl::SetNewOpenInfos(RangeInfoForEdit& rangeEdit, uint64_t& file_number, uint64_t& offset1, uint64_t& index_offset, uint64_t& holeSize, bool isSStable){
  mutexS_.Lock();//将修改共享变量 next_file_number_ pending_outputs_
  file_number = versions_->NewFileNumber();  //return next_file_number_++
  pending_outputs_.insert(file_number);
  mutexS_.Unlock();

  rangeEdit.Set_file_number(file_number);
  offset1 = 0;
  if(isSStable) {
    index_offset = 0; //offset1 和 index_offset(或称为offset2)都设置为0进行的是SStable的创建
    holeSize = 0;//该值于SStable可设置为任意值
  }
  else {
    index_offset = config::fileCommonSize;
    holeSize = config::fileCommonSize;
  }
}

//将rangeEdit.smallest设置为existRange的smallest;
//并返回existRange的file_number(该file_number指示的文件无需保护,因为其在当前版本中必定存在不会被删除),offset1, index_offset(offset2), holeSize
void DBImpl::SetExistOpenInfos(RangeInfoForEdit& rangeEdit, uint64_t& file_number, uint64_t& offset1, uint64_t& index_offset, uint64_t& holeSize, const RangeMetaData* existRange) {
  rangeEdit.Set_smallest(existRange->smallest);
  file_number = existRange->filemeta->file_number;
  offset1 =  existRange->offset1;
  index_offset = existRange->index_offset;
  holeSize = existRange->holeSize;
}

//返回true表示结束taskProcedure->builder中表示的MSStable的构建(使得同一user key的多条记录不跨range存储)
//input->key()是下一个待写到下一层的数据的key
bool DBImpl::ConditionForFinishFile(const TaskProcedureState* taskProcedure, const Iterator* input, int openedChildIdx, const std::string& current_user_key, const std::string& firstKeyInsert) {
  assert(taskProcedure != NULL);
  if(taskProcedure->builder == NULL) return false; //对于没有open过的自然也就没有finish,所以返回false
  if(!input->Valid())  return true;  //输入的迭代器已经完毕, 自然应该结束了


  TaskInfo::TaskType tasktype = taskProcedure->taskinfo->tasktype();
   //imm的combine-shift操作，只有在input迭代器(即Imm的迭代器)遍历完了才返回ture
  if(tasktype == TaskInfo::tCombineShift && taskProcedure->taskinfo->isMemTask()) {
    return false;
  }
  else if( (tasktype == TaskInfo::tCombineFlush || tasktype == TaskInfo::tAdjustmentFlush) ) { //flush操作：flush到稳定层或非稳定层
    //对于flush到非稳定层:
      //若当前的孩子"未满"或下一个孩子"未满"且下一个要插入的key离下一个孩子的范围近 或 进入该范围 或 跳过了该的范围(这里的范围比较都用user key进行),则返回true
    //对于flush到稳定层：下一个要插入的key大于根据"现对孩子的孩子数进行划分"和"划分完孩子的孩子后求孩子的孩子的中间键"策略获得的键,则返回true
    assert(openedChildIdx >= 0);
    if(openedChildIdx < taskProcedure->taskinfo->inputs(1).size()-1 && \
       !taskProcedure->divideChildUserkeys[openedChildIdx].empty() &&  \
       internal_comparator_.user_comparator()->Compare(ExtractUserKey(input->key()), taskProcedure->divideChildUserkeys[openedChildIdx] ) > 0 )//不能取==,因为divide key向下取整可能等于临界值
      return true;

//    if(taskProcedure->taskinfo->isFlushToInstable && taskProcedure->cOutputType() == TaskProcedureState::newed) { //最后一层不满的也会分裂，此时最后一层的计数值需要改变(暂不这么处理)
    if(taskProcedure->taskinfo->isFlushToInstable && taskProcedure->taskinfo->input(1, openedChildIdx)->isFull() && taskProcedure->cOutputType() == TaskProcedureState::newed) {//最后一层不满的不会分裂
      //新建的MSStable不能结束的条件是已经达到可增多的最大值
      if(taskProcedure->newedNumForInstable == taskProcedure->taskinfo->maxNewedNumForInstable)  return false;

      //若下面要插入的key的user key与上一个相同则结束当前的ragne而开始另一个以使得同一user key的所有记录存储在同一个range中(上面返回true则不可能相同)
      ParsedInternalKey ikey;
      Slice key = input->key();
      ParseInternalKey(key, &ikey); //不管是否解析成功(若解析失败一般当作不同的)
      if(current_user_key == ikey.user_key) return false;

      //新建的MSStable结束条件是插入的满一半(因为FileSize返回的是offset1,并不将索引数据计算在内,可以保证range增加的个数<=taskinfo中记录的最大值)
      if(taskProcedure->builder->CurrentFileSize() >= config::instableRangeInitialDataSize )  return true;
    }
    assert(taskProcedure->cOutputType() != TaskProcedureState::invalid);
    return false;
  }
  else if (tasktype == TaskInfo::tSplit) { //分裂操作
     //达了第二个文件,第二个文件的结束条件只能上面input变为invalid了
    if(internal_comparator_.user_comparator()->Compare(firstKeyInsert, taskProcedure->taskinfo->divideUserkeyForSplit ) > 0 )
      return  false;
     //第一个文件的结束条件,不能取==,因为divide key向下取整可能等于临界值
    else if (internal_comparator_.user_comparator()->Compare(ExtractUserKey(input->key()), taskProcedure->taskinfo->divideUserkeyForSplit ) > 0 )
      return true;
    else //两个都小于divideChildUserkeys
      return false;
  }
  else if(tasktype = TaskInfo::tGarbageCollection) {
    assert(taskProcedure->cOutputType() == TaskProcedureState::newed);
    if(taskProcedure->newedNumForInstable == taskProcedure->taskinfo->maxNewedNumForInstable)  return false;
    //若下面要插入的key的user key与上一个相同则结束当前的ragne而开始另一个以使得同一user key的所有记录存储在同一个range中
    ParsedInternalKey ikey;
    Slice key = input->key();
    ParseInternalKey(key, &ikey); //不管是否解析成功(若解析失败一般当作不同的)
    if(current_user_key == ikey.user_key) return false;
    //新建的MSStable结束条件是插入的满一半(因为FileSize返回的是offset1,并不将索引数据计算在内,可以保证range增加的个数<=taskinfo中记录的最大值)
    if(taskProcedure->builder->CurrentFileSize() >= config::instableRangeInitialDataSize )  return true;
    return false;
  } else {
    assert(0);
  }

}

//结束TaskProcedureState->builder的MSStable(有同步,同步完关闭):
  //若被finish成功, 更新taskProcedure->currentRangeEdit()的offset1, index_offset_end, index_offset, holeSize, appendTimes, usePercent;
  //若finish发现空间只够写索引,析构taskProcedure->builder 和 outfile并都置为NULL, 将taskProcedure组织的RangeInfoForEdit队列的最后一个pop掉,直接返回;
//更新*taskProcedure中的状态:析构taskProcedure->builder 和 outfile(同步写磁盘并关闭文件)并都置为NULL; input只是用来check是否有iterator error
Status DBImpl::FinishTaskOutputFile(TaskProcedureState* taskProcedure, Iterator* input, int openedChildIdx, bool isNewed, uint64_t file_number) {

  assert(taskProcedure != NULL && taskProcedure->outfile != NULL || taskProcedure->builder != NULL);
  // Check for iterator errors
  Status s = input->status();

  const uint64_t current_entries = taskProcedure->builder->NumEntries();
  if (s.ok()) {
    s = taskProcedure->builder->Finish();
  } else {
    taskProcedure->builder->Abandon();
  }

  if(s.IsFileHoleOnlyHoldIndex() || s.IsFileHoleSmall() ) { //发现剩余的空间只存储得下索引(存储不下的话MtableBuilder上面并没有写文件) 或 连索引的存不下
    delete taskProcedure->builder;
    taskProcedure->builder = NULL;
    delete taskProcedure->outfile; //会关闭文件
    taskProcedure->outfile = NULL;
    taskProcedure->PopBackOneOutput();
    return s;
  }

  //若被正常的finish成功, 更新taskProcedure->currentRangeEdit()的offset1, index_offset_end, index_offset, holeSize, appendTimes, usePercent
  RangeInfoForEdit* rangeEdit = taskProcedure->currentRangeEdit();
  assert(rangeEdit != NULL);
  if(s.ok()) {
    if (isNewed){
      rangeEdit->Set_index_offset_end(config::fileCommonSize);  //对于SStable(Imm的shift操作中出现),这样设置不正确,下面会重新设置
      rangeEdit->Set_appendTimes(1);
    }
    else{
      rangeEdit->Set_appendTimes(taskProcedure->taskinfo->inputs(1)[openedChildIdx]->appendTimes + 1);
    }
    //对于imm的combine-shift操作,因为写入的是SStable格式，更新rangeEdit的index_offset_end
    if(taskProcedure->taskinfo->tasktype() == TaskInfo::tCombineShift && taskProcedure->taskinfo->isMemTask()) {
      assert(isNewed);
      std::string fname = MtableFileName(dbname_, rangeEdit->Get_file_number());
      uint64_t indexOffsetEnd;
      s = env_->GetFileSize(fname, &indexOffsetEnd); assert(s.ok());
      rangeEdit->Set_index_offset_end(indexOffsetEnd);
    }

    rangeEdit->Set_offset1( taskProcedure->builder->Offset1() );
    rangeEdit->Set_index_offset( taskProcedure->builder->Index_offset() );
    rangeEdit->Set_holeSize(taskProcedure->builder->HoleSize());
    rangeEdit->Set_usePercentByCompute();  //需要设置了holeSize_, index_offset_, offset1才可调用
  }
  const uint64_t current_bytes = taskProcedure->builder->FileSizeByThisBuilder();
  taskProcedure->total_bytes += current_bytes;
  taskProcedure->total_bytes_exclude_meta += taskProcedure->builder->FileSizeByThisBuilder(false);
  delete taskProcedure->builder;
  taskProcedure->builder = NULL;

  // Finish and check for file errors
  // 文件同步到磁盘
  if (s.ok()) {
    s = taskProcedure->outfile->Sync();
  }
  if (s.ok()) {
    s = taskProcedure->outfile->Close();
  }
  delete taskProcedure->outfile;
  taskProcedure->outfile = NULL;


  if (s.ok() && current_entries > 0) {
    // Verify that the table is usable //会加入Cache(不管readoption的设置如何一定为加入table cache,block cache配置了但是没用因为没有迭代)
    Iterator* iter = mtable_cache_->NewIterator(ReadOptions(),
                                               file_number,
                                               isNewed ? rangeEdit->Get_index_offset_end() : config::fileCommonSize,
								               rangeEdit->Get_index_offset(),
                                               rangeEdit->Get_appendTimes());
    s = iter->status();
    delete iter;

    std::string tmp;
    if(isNewed) tmp = "Generated table #%llu: %lld keys, %lld bytes";
    else tmp = "modified table #%llu: %lld keys, %lld bytes";

    if (s.ok()) {
      Log(options_.info_log,  //写日志不需要加锁，因为FILE的所有操作都是原子的(这里需要加锁吗?)
          tmp.c_str(),
          (unsigned long long) file_number,
          (unsigned long long) current_entries,
          (unsigned long long) current_bytes);
    }

  }
  return s;

}

//任务类型：1. 处理的是Immutable Memtable 下移任务, combine-shift类型或combine-flush类型，对于combine-flush还得判断下一层是否为非为稳定层
//          2. 处理的flush任务，注意下一层是否为非稳定层
//          3. 处理的是split任务
//          4. 处理的是磁盘range的shift(combine/data-shift)任务，无需磁盘操作(Imm的combine-flush除外),将input[0]_插入新建的output队列中, 直接返回
//          5. 处理的是Garbage collection任务
//操作后的信息记录在taskProcedure中, 这里记录"孩子的"modified/newed range, deleted range以及父节点无需记录其可以根据任务类型和input_[2]推断出来,具体见AddInputDeletions函数
//除了LogAndAplly和用户的log这里是唯一的写磁盘的地方(除了其他的可忽略的磁盘操作)
Status DBImpl::DoMainDiskJob(TaskProcedureState* taskProcedure) {
  const uint64_t start_micros = env_->NowMicros();//返回当前的时间，从1970年1月1日00：00：00开始(以微秒为单位) ,多线程安全
  Status status;
  TaskInfo* taskinfo = taskProcedure->taskinfo;

  Log(options_.info_log,  " task type: %d, tasking imm/disk_rges %d/%d@%d + %d@%d ranges",  //写日志不需要加锁，因为FILE的所有操作都是原子的
      taskinfo->tasktype(),
      taskinfo->isMemTask(),
      taskinfo->num_input_files(0),
      taskinfo->level(),
      taskinfo->num_input_files(1),
      taskinfo->immShiftLevel== -1 ? taskinfo->level() + 1 : taskinfo->immShiftLevel);

  //磁盘range的combine-shift/data-shift任务，无需磁盘操作(Imm的combine-flush除外),根据input[0]_构建RangeInfoForEdit插入新建的output队列中, 直接返回
  if( (taskinfo->tasktype() == TaskInfo::tCombineShift || taskinfo->tasktype() == TaskInfo::tDataShift) && !taskinfo->isMemTask()) {
     for(int i = 0; i < taskinfo->inputs(0).size(); i++) {
       //使用之前的filemeta以使得每个文件(或说file number)有且只有一个filemeta与之对应
       RangeInfoForEdit rangeEdit(*taskinfo->input(0,i));
       taskProcedure->pushBackOneOutput(rangeEdit, TaskProcedureState::newed); //插入新建的output队列中
     }
     return status;
  }
  assert(taskProcedure->builder == NULL);
  assert(taskProcedure->outfile == NULL);

  mutexS_.Lock();

  //设置taskProcedure->smallest_snapshot
  if (snapshots_.empty()) {
    taskProcedure->smallest_snapshot = versions_->LastSequence();//最新的（或上一个）sequence number，即InternalKey中的
  } else {
    taskProcedure->smallest_snapshot = snapshots_.oldest()->number_;
  }

  //对于flush到稳定层的任务设置taskProcedure->divideChildUserkeys(taskinfo中为split记录的分裂键在构造任务时已经设置完毕) 和 optStableChildLimit 
  if(taskinfo->isFlushTask() && !taskinfo->isFlushToInstable) //flush到稳定层
    SetDivideKeysForFlushStable(taskProcedure);

  // Release mutex while we're actually doing the task
  mutexS_.Unlock();

  //对于flush到非稳定层的任务设置taskProcedure->divideChildUserkeys(taskinfo中为split记录的分裂键在构造任务时已经设置完毕)
  if(taskinfo->isFlushToInstable) {
    SetDivideKeysForFlushInstable(taskProcedure);
  }

  TaskStats stats;
  std::vector<RangeMetaData*>& childs = taskinfo->inputs(1);
  //生成的迭代器效果与为c->inputs_[0]/imm,[1]下组织的所有文件进行归并生成新的迭代器等价
  Iterator* input = versions_->MakeInputIterator(taskinfo);
  input->SeekToFirst();
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;//56位全为1
  int openedChildIdx = -1; //当前处理的孩子range的索引, 只对flush有效
  bool isNewed;  //当前要插入的range是新建的还是打开原来的
  InternalKey tmpInternalKey; //存储当前的key
  std::string firstKeyInsert; //保存的是当前打开的文件插入的第一条key,当finish失败时使用以重新插入
  uint64_t file_number = -1;  //当前打开的文件的编号
  bool normalAppend2hole= true;   //下一个写入文件采用正常的append策略,即索引数据和用户数据都写进文件的洞里
  for (; input->Valid(); ) {
    Slice key = input->key(); //返回的是internal key的编码
    // Handle key/value, add to state, etc.
    bool drop = false;
    if (!ParseInternalKey(key, &ikey)) {//解析key不成功(后面将add到目的SStable文件中, 不丢弃)
      // Do not hide error keys
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;//56位全为1
    } else {                            //解析key成功
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) != 0) {
        // First occurrence of this user key(第一次出现的userkey)
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;//第一次出现的userkey 将last_sequence_for_key保存为"正无穷":以防止被drop
      }

      //注意因为sequence number大的(后插入的)先被迭代到，所以下面条件满足等价于 对于第二次(或更多次)出现同一userkey(在此次merge中)的记录
      //若上一个user key对应的sequence number <= compact->smallest_snapshot 将被drop
      //last_sequence_for_key 保存着同一userkey的上一个key的sequence number(大于当前key的),对于第一个其上一个为正无穷
      if (last_sequence_for_key <= taskProcedure->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;    // (A)
      }/*  else if (ikey.type == kTypeDeletion &&  
                 ikey.sequence <= taskProcedure->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {//user_key在level_+2层及以下没有overlap该user_key的文件
        // For this user key:
        // (1) there is no data in higher levels(即leve+2及以上没有overlap该user key的SSTable:上面的条件3, 否则drop本条记录，读会出错)
        // (2) data in lower levels will have larger sequence numbers(当然也可能没有,最主要是没有更小的)
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      } */
      //上面的IsBaseLevelForKey需要加S锁,而且作用很有限,而且若需采用此逻辑也需要做较大修改如对于append到本range以及分裂操作,所以去掉此逻辑

      last_sequence_for_key = ikey.sequence; //第二次 或更多次后的同一userkey时设置
    }

    if (!drop) {//不应丢弃
      // Open output file if necessary(第一个或上一个MSStable满后)
      if (taskProcedure->builder == NULL) {
        //结果有三种:新建SStable(只对Imm的shift);新建MSStable(flush到非稳定层需要重写的和split两种操作);打开原来的MSStable(flush):
          //首先根据任务类型判断是modified还是newed,对于newed需要获得file_number并插入pending_outputs_;
          //构建RangeInfoForEdit,若是modified设置其smallest,若是newed设置其file_number
          //打开原来的或新的range文件, 更新初始化taskProcedure.outfile,builder,对于flush到非稳定层而新增range则taskProcedure->newedNumForInstable++
        status = OpenTaskOutputFile(taskProcedure, key, openedChildIdx, isNewed, file_number, normalAppend2hole);
        firstKeyInsert = key.ToString(); //注意不能用Slice因为key对应的空间可能已经释放
        if (!status.ok()) {
          break;
        }
      }
      //可能设置smallest, old_smallest 
      tmpInternalKey.DecodeFrom(key);
      if (taskProcedure->builder->NumEntries() == 0) {//调用了Add的函数的次数, 即没有调用过Add函数
        if(isNewed || !isNewed && internal_comparator_.Compare(tmpInternalKey, taskProcedure->currentRangeEdit()->Get_smallest()) < 0 ) {
          if(!isNewed) {
            taskProcedure->currentRangeEdit()->Set_old_smallest(taskProcedure->currentRangeEdit()->Get_smallest());
          }
          if(!isNewed && taskinfo->isFlushTask() && !taskinfo->isFlushToInstable && \
             openedChildIdx > 0 && !taskProcedure->optStableChildLimit[openedChildIdx-1].second.empty() && \
             user_comparator()->Compare(taskProcedure->optStableChildLimit[openedChildIdx-1].second, tmpInternalKey.user_key()) < 0) {
            //flush到稳定层的情况，smallest limit更小, 则用该limit设置
            InternalKey optLimitSmallestKey(taskProcedure->optStableChildLimit[openedChildIdx-1].second, kMaxSequenceNumber, kTypeValue);
            taskProcedure->currentRangeEdit()->Set_smallest(optLimitSmallestKey);
          } else {
            taskProcedure->currentRangeEdit()->Set_smallest(tmpInternalKey);
          }
        }
      }

      //将记录插入打开的文件
      taskProcedure->builder->Add(key, input->value()); //caixin: Internalkey ,value
    }

    input->Next();

    // 如果条件满足关闭已经打开的MSStable文件(使得同一user key的多条记录不跨range存储), 可能设置largest
    if ( ConditionForFinishFile(taskProcedure, input, openedChildIdx, current_user_key, firstKeyInsert) ) { //若没有记录插入即没有打开本函数也返回false
      if(isNewed || !isNewed && internal_comparator_.Compare(tmpInternalKey, childs[openedChildIdx]->largest) > 0) {
        if(!isNewed && taskinfo->isFlushTask() && !taskinfo->isFlushToInstable && \
           openedChildIdx < taskinfo->num_input_files(1)-1 && !taskProcedure->optStableChildLimit[openedChildIdx].first.empty() && \
           user_comparator()->Compare(taskProcedure->optStableChildLimit[openedChildIdx].first, tmpInternalKey.user_key()) > 0) {
          //flush到稳定层的情况，largest limit更大, 则用该limit设置
          InternalKey optLimitLargestKey(taskProcedure->optStableChildLimit[openedChildIdx].first, kMaxSequenceNumber, kTypeValue);
          taskProcedure->currentRangeEdit()->Set_largest(optLimitLargestKey);
        } else {
          taskProcedure->currentRangeEdit()->Set_largest(tmpInternalKey);
        }
      }
      //结束TaskProcedureState->builder的MSStable(有同步,同步完关闭):
        //若被finish成功, 更新taskProcedure->currentRangeEdit()的offset1, index_offset_end, index_offset, holeSize, appendTimes, usePercent;
        //若finish发现空间只够写索引,析构taskProcedure->builder 和 outfile并都置为NULL, 将taskProcedure组织的RangeInfoForEdit队列的最后一个pop掉,直接返回;
      //更新*taskProcedure中的状态:析构taskProcedure->builder 和 outfile(同步写磁盘并关闭文件)并都置为NULL; input只是用来check是否有iterator error
      status = FinishTaskOutputFile(taskProcedure, input, openedChildIdx, isNewed, file_number);
      if(status.ok()) {
        normalAppend2hole = true;
      }
      else if(status.IsFileHoleOnlyHoldIndex()) { //可能是原有的，也可能是新键的文件(isChildMerge[openedChildIdx] == true而新建)
        input->Seek(firstKeyInsert); //重新插入
        normalAppend2hole = false;
        status = Status::OK();
      }
      else if(status.IsFileHoleSmall()) {
     //直接采用归并生成新的比较简单一些(虽然可加一种文件格式使还为append,但相对复杂),写到不满的孩子才可能出现洞写不下文件的情况(不考虑大量记录使一个新建的文件洞或空的文件中连索引都存不下,后者也就不用更新空文件数目的信息了) 
        assert(!taskinfo->input(1,openedChildIdx)->isFull());
        delete input;
        input = NULL;
        taskinfo->isChildMerge[openedChildIdx] = true; //会被重写
        input = versions_->MakeInputIterator(taskinfo);
        if(taskProcedure->isRangeEditEmpty() ) {
          assert(openedChildIdx == 0); //写到不满的孩子才可能出现洞写不下文件的情况, 此时openedChildIdx意义正确
          input->SeekToFirst();
        }
        else { //一定写到的是不满的孩子，故即使是最后一层,divideChildUserkeys一定有效，此时利用此信息
          assert(openedChildIdx >= 1 && openedChildIdx <= taskinfo->inputs(1).size()-1 && !taskProcedure->divideChildUserkeys[openedChildIdx-1].empty());
          //其中记录中sequence number最小为1(DBImpl::Write函数中看出,插入的记录的sequence number为last_sequence_+1,其中last_sequence_初始化为0),通过构造sequence number为0的记录,seek时可以跳过此InternalKey对应的userkey的所有记录
          InternalKey seekPoint(taskProcedure->divideChildUserkeys[openedChildIdx-1], 0, kTypeValue); //注意若只seek到firstKeyInsert可能会丢失孩子的部分记录
          input->Seek(seekPoint.getString());
        }

        normalAppend2hole = false; //其实虽然后面会新建一个文件重写，但是一般情况下新文件的洞也只存放得下索引, 故索引写进hole里，用户记录append到文件的末尾(其实可以采用SStable的格式,但是这里没这样做因为可能后面会去掉SStable格式)
        status = Status::OK();

        Log(options_.info_log,  "\n%s", "merge occurs because of hole too small"); //因为此情况应该发生的极少，甚至不发生,若出现可以提示一下用户
      }
      else if (!status.ok()) { //其他异常不继续插入
        break;
      }
    }

  }

  if (status.ok()) {
    status = input->status();
  }
  delete input; //迭代器
  input = NULL;

  //更新state_(state_[i]记录的是生成本层数据的操作的花费) , RangeMetaData
  stats.micros = env_->NowMicros() - start_micros;
  for (int i = 0; i < taskinfo->num_input_files(0); i++) {
    stats.bytes_read += taskinfo->input(0, i)->DataMount();
  }
  if(taskinfo->isMemTask() || taskinfo->input(0,0)->usePercent>0) {
    for (int i = 0;  (taskinfo->isFlushToInstable || taskinfo->isFlushTask()) && i < taskinfo->num_input_files(1); i++) {
      RangeMetaData* range = taskinfo->input(1, i);
      if( range->isFull() || taskinfo->isChildMerge[i] )
        stats.bytes_read += range->DataMount();
    }
  }
  stats.bytes_written += taskProcedure->total_bytes;

  mutexS_.Lock(); //更新stats_前加锁, 当然可以将本参数传出去在其他地方加锁设置以减少加锁的次数
  if(taskinfo->tasktype() == TaskInfo::tSplit || taskinfo->tasktype() == TaskInfo::tGarbageCollection)
    stats_[taskinfo->level()].Add(stats);
  else{ //imm的shift操作和flush操作
    if(taskinfo->immShiftLevel != -1) //imm 的shift操作
      stats_[taskinfo->immShiftLevel].Add(stats);
    else //flush操作(磁盘上的shift操作该值为0)
      stats_[taskinfo->level() + 1].Add(stats);
  }
  mutexS_.Unlock(); //解锁

  return status;
}

//需持有S锁(因为会使用current版本信息内的其他未被加R锁的range信息)
//对于flush到稳定层的任务设置taskProcedure->divideChildUserkeys 和 optStableChildLimit(因为本函数较为晦涩,文档中有示意图和文字说明)
void DBImpl::SetDivideKeysForFlushStable(TaskProcedureState* taskProcedure) {
  //flush任务需设置taskProcedure->divideChildUserkeys
  assert(taskProcedure->taskinfo->isFlushTask() && !taskProcedure->taskinfo->isFlushToInstable); //flush到稳定层
  int level = taskProcedure->taskinfo->level();
  const std::vector<RangeMetaData*>(&ranges) [config::kNumLevels] = versions_->current()->ranges(); //加了S锁current版本调用此函数期间不会被替换
  std::vector<RangeMetaData*>& childs = taskProcedure->taskinfo->inputs(1);
  std::vector<std::pair<std::vector<RangeMetaData*>, std::pair<int,int> > > grandchildsVector;
  for(int i = 0;  i < childs.size(); ++i) {
    std::vector<RangeMetaData*> grandChilds;
    std::pair<int, int> twoEndIdx;
    //将level层中overlap [user begin,user end]的所有文件写入inputs(可能多个); 
    //若twoEndIdx不为NULL，将孩子的最小和最大索引存入，若不存在孩子则为(j,j),j为the smallest index j such that ranges[leve+2][j]->largest >= childs[i].smallest.user_key()
    versions_->current()->GetOverlappingInputs(level+2, &childs[i]->smallest, &childs[i]->largest, &grandChilds, &twoEndIdx);
    grandchildsVector.push_back(std::make_pair(grandChilds, twoEndIdx));
  }
  taskProcedure->divideChildUserkeys.clear();
  for(int i = 0; i < childs.size()-1; ++i) {
    std::string divideChildUserkey;
    int childNumA = grandchildsVector[i].first.size();
    int childNumB = grandchildsVector[i+1].first.size();
    int childA_startIdx = grandchildsVector[i].second.first;
    int childA_endIdx = grandchildsVector[i].second.second;
    int childB_startIdx = grandchildsVector[i+1].second.first;
    int childB_endIdx = grandchildsVector[i+1].second.second;
    //tmpA将设置为[划分给]childA的最大孩子的largest.user_key()和childA.largest.user_key()的更大值;
    //tmpB将设置为[划分给]childB的最小孩子的smallest.user_key()和childB.smallest.user_key()的更小值;
    //若childA和childB有公共孩子则不设置
    Slice tmpA, tmpB;
    std::string largest_limit, smallest_limit;
    if(childNumA > 0 && childNumB > 0 && childA_endIdx != childB_startIdx ) { //两个都有孩子 且 没有公共的孩子
      while(childA_endIdx < childB_startIdx - 1) { //存在待被划分的孙子
        if(childNumA <= childNumB){
          childNumA++;
          childA_endIdx++;
        }
        else {
          childNumB++;
          childB_startIdx--;
        }
      }
      assert(childA_endIdx == childB_startIdx-1);
      tmpA = user_comparator()->Compare(childs[i]->largest.user_key(), ranges[level+2][childA_endIdx]->largest.user_key()) > 0 ? \
                                              childs[i]->largest.user_key() : ranges[level+2][childA_endIdx]->largest.user_key();
      tmpB = user_comparator()->Compare(childs[i+1]->smallest.user_key(), ranges[level+2][childB_startIdx]->smallest.user_key()) < 0 ? \
                                              childs[i+1]->smallest.user_key() : ranges[level+2][childB_startIdx]->smallest.user_key();
      //到现在没有被划分的孙子已经被划分, 根据前一个的最大孙子最大key和后一个的最小孙子的最小key设置divideChildUserkey
      divideChildUserkey = user_comparator()->MiddleString(tmpA, tmpB);
    } else { //两个都没有孩子 或 一个没有孩子 或 两个都有孩子但有一个属于公共的孩子
      //根据前一个孩子的最大key和后一个孩子的最小键的中间key设置divideChildUserkey
      //idxA,B 为the smallest index idxA,B such that ranges[level+2][idxA,B]->largest >= childs[i/i+1]->smallest.user_key(). 只在chids[i/i+1]没有孩子的情况使用idxA/B,故等价于
      //------------------------------------------------------------------------------------------------largest-----------
      int idxA = childA_startIdx, idxB =  childB_startIdx;
      assert(idxB - idxA >= 0);
      if(childNumA == childNumB && 0 == childNumA) { //两个都没有孩子
        //[idxA, idxB-1]为待划分的(对于没有待划分的孩子的情况下面也都成立),其个数为idxB-idxA, 左边的划分(idxB-idxA+1)/2个孩子 右边的划分(idxB-idxA)/2孩子
        tmpA = ((idxB-idxA+1)/2 > 0) ? ranges[level+2][idxA-1 + (idxB-idxA+1)/2]->largest.user_key() : childs[i]->largest.user_key();
        tmpB = ((idxB-idxA)/2 > 0) ? ranges[level+2][idxB - (idxB-idxA)/2]->smallest.user_key() : childs[i+1]->smallest.user_key();
        divideChildUserkey = user_comparator()->MiddleString(tmpA, tmpB);
      } else if(childNumA == 0 || childNumB == 0) {  //有且只有一个有孩子, 一个没孩子
        if(childNumA > 0) { //左边有孩子,两个组成的总的孩子数idxB-childA_startIdx，左边max{childNumA, (idxB-childA_startIdx+1)/2},右边为剩下的孩子
          int totalChildNum = idxB-childA_startIdx; //两个range将拥有的最大"孩子数之和"
          int leftChildNum = (totalChildNum+1)/2 >  childNumA ? (totalChildNum+1)/2 : childNumA;
          int rightChildNum = totalChildNum - leftChildNum;
          assert(rightChildNum >= 0);
          tmpA = user_comparator()->Compare(childs[i]->largest.user_key(), ranges[level+2][childA_startIdx-1+leftChildNum]->largest.user_key()) > 0 ? \
                                              childs[i]->largest.user_key() : ranges[level+2][childA_startIdx-1+leftChildNum]->largest.user_key();
          tmpB = (rightChildNum == 0) ? childs[i+1]->smallest.user_key() : ranges[level+2][idxB-rightChildNum]->smallest.user_key();
        } 
        else if(childNumB > 0) { //右边有孩子
          int totalChildNum = childB_endIdx - idxA + 1;  ///两个range将拥有的最大"孩子数之和"
          int rightChildNum = totalChildNum/2 > childNumB ? totalChildNum/2 : childNumB;
          int leftChildNum = totalChildNum - rightChildNum;
          assert(leftChildNum >= 0 && idxA < ranges[level+2].size());
          tmpB = user_comparator()->Compare(childs[i+1]->smallest.user_key(), ranges[level+2][childB_endIdx+1-rightChildNum]->smallest.user_key()) < 0 ? \
                                                childs[i+1]->smallest.user_key() : ranges[level+2][childB_endIdx+1-rightChildNum]->smallest.user_key();
          tmpA = (leftChildNum == 0) ? childs[i]->largest.user_key() : ranges[level+2][idxA-1+leftChildNum]->largest.user_key();

        } else {
          assert(0);
        }
        divideChildUserkey = user_comparator()->MiddleString(tmpA, tmpB);
        
      } else { //有公共孩子
        assert(childNumA > 0 && childNumB > 0 && childA_endIdx == childB_startIdx);
        divideChildUserkey = user_comparator()->MiddleString( childs[i]->largest.user_key() , childs[i+1]->smallest.user_key() );
      }
    }
    taskProcedure->divideChildUserkeys.push_back(divideChildUserkey);
    taskProcedure->optStableChildLimit.push_back(std::make_pair(tmpA.ToString(), tmpB.ToString()));
  }
}

//不需持有S锁
//对于flush到非稳定层的任务设置taskProcedure->divideChildUserkeys(其中可能有空的字符串表示无需对其进行比较)
void DBImpl::SetDivideKeysForFlushInstable(TaskProcedureState* taskProcedure) {
  std::vector<RangeMetaData*>& childs = taskProcedure->taskinfo->inputs(1);
  assert(taskProcedure->taskinfo->isFlushToInstable);
  for(int i = 0; i < childs.size()-1; ++i) {
    std::string divideChildUserkey;
    if(!childs[i]->isFull() || !childs[i+1]->isFull()) {
      divideChildUserkey = user_comparator()->MiddleString( childs[i]->largest.user_key(),  childs[i+1]->smallest.user_key() );
    }
    taskProcedure->divideChildUserkeys.push_back(divideChildUserkey); //若为空的string无需对其比较(或理解为最大)
  }
  for(int i = childs.size()-2; i >= 0; --i) {
    int lastMidKeyidx = -1;
    if(!taskProcedure->divideChildUserkeys[i].empty()) lastMidKeyidx = i;
    else if(lastMidKeyidx != -1) taskProcedure->divideChildUserkeys[i] = taskProcedure->divideChildUserkeys[lastMidKeyidx];
    else continue;
  }
}

//根据taskProcedure(taskProcedure在DoMainDiskJob中进行设置或无需进行DoMainDiskJob)设置applyinfo （无需持有S锁）
void DBImpl::SetApplyinfo(TaskProcedureState* taskProcedure, ApplyInfo* applyinfo) {
  TaskInfo* taskinfo = taskProcedure->taskinfo;
  TaskInfo::TaskType tasktype = taskinfo->tasktype();
  //对于imm的combine-shift操作, 设置applyinfo的addingNum, newFullLevel, newFulls三个成员, 以及 dataDeltaLevel2, dataDeltaAmount2
  if(tasktype == TaskInfo::tCombineShift && taskinfo->isMemTask()) {
    assert(taskinfo->immShiftLevel >= 0 && taskinfo->immShiftLevel <= config::kMaxMemShiftLevel);
    assert(!taskProcedure->currentRangeEdit()->Get_smallest().isEmpty()); //smallest必定被设置
    applyinfo->addingNum = std::make_pair(taskinfo->immShiftLevel, 1); //immShiftLevel增加了1个range
    applyinfo->newFullLevel = taskinfo->immShiftLevel;
    applyinfo->newFulls.insert( taskProcedure->currentRangeEdit()->Get_smallest());
    //不用设置applyinfo 的 oldEmptyLevel1/2, oldEmptyNum1/2, newEmptyLevel, newEmptyNum
    applyinfo->dataDeltaLevel2 = taskinfo->immShiftLevel;
    applyinfo->dataDeltaAmount2 = taskProcedure->total_bytes_exclude_meta;

  }
  //对于所有其他操作设置applyinfo
  else if(tasktype == TaskInfo::tAdjustmentFlush || tasktype == TaskInfo::tCombineFlush || \
          tasktype == TaskInfo::tCombineShift || tasktype == TaskInfo::tDataShift ||    \
          tasktype == TaskInfo::tSplit || tasktype == TaskInfo::tGarbageCollection) { //flush(到非稳定层或稳定层)或shift或split或garbage collection
    std::vector<RangeMetaData*>& parents = taskinfo->inputs(0);
    std::vector<RangeMetaData*>& childs = taskinfo->inputs(1);

    //设置addingNum和reducingNum
    if (tasktype == TaskInfo::tCombineShift) { //磁盘上的range的combine-shift操作
      applyinfo->reducingNum = std::make_pair(taskinfo->level(), parents.size());
      applyinfo->addingNum = std::make_pair(taskinfo->level()+1, parents.size());
    }
    else if( tasktype == TaskInfo::tDataShift) {
      assert(parents.size() == 1);
      applyinfo->addingNum = std::make_pair(taskinfo->level()+1, parents.size());
    }
    else if(tasktype == TaskInfo::tSplit) {
      applyinfo->addingNum = std::make_pair(taskinfo->level(), 1);
    }
    else if(tasktype == TaskInfo::tCombineFlush && taskinfo->level()>=0){
      applyinfo->reducingNum = std::make_pair(taskinfo->level(), 1);
    }

    //设置applyinfo的oldFullLevel,oldFulls
    const std::vector<RangeMetaData*>* oldRanges[2];
    oldRanges[0] = &parents;
    oldRanges[1] = &childs;
    //对于flush没数据的range(只发生在磁盘的range上,其实只可能为combine-flush)的情况是不用设置old full的(其他都要设置),因为孩子即使满也没有变化
    //即 对于非flush任务(如tSplit) 和 有数据的flush任务,满的range信息才需要变化(与MakeInputIterator 和 setVersionEdit函数一致)
    if ( !taskinfo->isFlushTask() || (taskinfo->isMemTask() || (*oldRanges[0])[0]->usePercent>0) ) {
      for (int i = 0; i < 2; ++i) {
        for (std::vector<RangeMetaData*>::const_iterator cst_iter = (*oldRanges[i]).begin();  cst_iter != (*oldRanges[i]).end(); ++cst_iter) {
          if ((*cst_iter)->isFull()) {
            if (i==0) {
              applyinfo->oldFullLevel1 = taskinfo->level();
              applyinfo->oldFulls1.insert(*cst_iter);
            }
            else {//(i==1)
              applyinfo->oldFullLevel2 = taskinfo->level() + 1;
              applyinfo->oldFulls2.insert(*cst_iter);
            }
          }
        }
      }
    }
    //设置applyinfo的newFullLevel, newFulls(split操作,flush操作)
    const std::vector<RangeInfoForEdit>* edits[2];
    edits[0] = taskProcedure->getModifiedOutputs();
    edits[1] = taskProcedure->getNewedOutputs();
    for(int i = 0; i < 2; ++i) {
      for(std::vector<RangeInfoForEdit>::const_iterator cst_iter = (*edits[i]).begin();  cst_iter != (*edits[i]).end(); ++cst_iter) {
        if(cst_iter->isFull()) {
          if(tasktype == TaskInfo::tSplit || tasktype == TaskInfo::tGarbageCollection)
            applyinfo->newFullLevel = taskinfo->level();
          else   //shift(imm combine-shift除外)或flush
            applyinfo->newFullLevel = taskinfo->level() + 1;
          applyinfo->newFulls.insert(cst_iter->Get_smallest() ); //插入的是新的smallest
        }
      }
    }
    //设置applyinfo的maxPureAddingNumForInstable
    if(taskinfo->isFlushToInstable || tasktype == TaskInfo::tGarbageCollection){
      applyinfo->maxPureAddingNumForInstable = taskinfo->maxPureAddingNumForInstable;
    }

     //设置applyinfo 的 oldEmptyLevel1,2, oldEmptyNum1,2, newEmptyLevel, newEmptyNum
    if(tasktype == TaskInfo::tCombineShift || tasktype == TaskInfo::tCombineFlush) { //combine操作的父亲节点若为空
      int num = parents.size();
      for(int i = 0; i < num; ++i) {
        if(parents[i]->DataMount() == 0) {
          applyinfo->oldEmptyLevel1 = taskinfo->level();
          applyinfo->oldEmptyNum1++;
        }
      }
      if(tasktype == TaskInfo::tCombineShift) {
        for(int i = 0; i < num; ++i) {
          if(parents[i]->DataMount() == 0) {
            applyinfo->newEmptyLevel = taskinfo->level() + 1;
            applyinfo->newEmptyNum++;
          }
        }
      }
    }

    if(tasktype == TaskInfo::tDataShift || tasktype == TaskInfo::tAdjustmentFlush) { //非combine操作的数据下移操作后，父亲节点变为空
      assert(parents.size() == 1);
      applyinfo->newEmptyLevel = taskinfo->level();
      applyinfo->newEmptyNum++;
    }

    if(tasktype == TaskInfo::tCombineFlush || tasktype == TaskInfo::tAdjustmentFlush) { //flush操作中，孩子节点若原来为空
      const std::vector<RangeInfoForEdit>* modifiedChild = taskProcedure->getModifiedOutputs();
      int num = modifiedChild->size();
      for(int i = 0; i < num; ++i) {
        if((*modifiedChild)[i].Get_appendTimes() == 1) { //说明之前是0
          applyinfo->oldEmptyLevel2 = taskinfo->level() + 1;
          applyinfo->oldEmptyNum2++;
        }
      }
      //原来为空的孩子会被删除(不属于modifiedChild)的情况只能是: 其被归并而且当新建的文件的洞连要写入数据的索引都存不下才会被归并而删除,空的文件会被删除而新建一个新的merge生成的文件(虽然可以不考虑，但是为了严谨这还是写上)
      //对于append和merge结合的情况,原来没有数据是不可能被merge的,只可能被append，不可能执行本情况
      for(int i = 0; i <  taskinfo->isChildMerge.size(); ++i) {
        if(taskinfo->isChildMerge[i] && childs[i]->appendTimes == 0) {
          applyinfo->oldEmptyLevel2 = taskinfo->level() + 1;
          applyinfo->oldEmptyNum2++;
        }
      }
    }

    //设置dataDeltaLevel1/2 dataDeltaAmount1/2
    if(tasktype == TaskInfo::tDataShift || tasktype == TaskInfo::tCombineShift) {
      applyinfo->dataDeltaLevel1 = taskinfo->level();
      applyinfo->dataDeltaLevel2 = taskinfo->level()+1;
      for(int i = 0; i < parents.size(); ++i) {
        applyinfo->dataDeltaAmount1 -= parents[i]->DataMount(false); //RangeMetaData
        applyinfo->dataDeltaAmount2 += parents[i]->DataMount(false);
      }
    }
    //有数据的flush任务,才需要变化(与MakeInputIterator 和 setVersionEdit函数一致)
    else if(taskinfo->isFlushTask() && (taskinfo->isMemTask() || parents[0]->usePercent > 0) ) {
      if(!taskinfo->isMemTask()) {
        applyinfo->dataDeltaLevel1 = taskinfo->level();
        applyinfo->dataDeltaAmount1 -= parents[0]->DataMount(false);
      }
      applyinfo->dataDeltaLevel2 = taskinfo->level()+1;
      applyinfo->dataDeltaAmount2 += taskProcedure->total_bytes_exclude_meta;
      for(int i = 0; i < childs.size(); ++i) {
        if(childs[i]->isFull() || taskinfo->isChildMerge[i]) {
          applyinfo->dataDeltaAmount2 -= childs[i]->DataMount(false);;
        }
      }
    }
    else if(tasktype == TaskInfo::tSplit || tasktype == TaskInfo::tGarbageCollection) {
      applyinfo->dataDeltaLevel1 = taskinfo->level();
      applyinfo->dataDeltaAmount1 -= parents[0]->DataMount(false);
      applyinfo->dataDeltaAmount1 += taskProcedure->total_bytes_exclude_meta;
    }
    else {
      assert(taskinfo->isFlushTask() && !taskinfo->isMemTask() && parents[0]->usePercent==0);
    }
    
  }
  else{
    assert(0);
  }
}

//根据taskProcedure, 设置taskProcedure->taskinfo.edit_, 注意对于Imm任务需要记录LogNumber
void DBImpl::setVersionEdit(TaskProcedureState* taskProcedure, bool isRecoverProcess) {
  Log(options_.info_log,  " task type: %d, Tasked imm/disk_rges %d/%d@%d + %d@%d ranges => %lld bytes", //写日志无需加锁，因为所有的FILE的操作都是原子的
      taskProcedure->taskinfo->tasktype(),
      taskProcedure->taskinfo->isMemTask(),
      taskProcedure->taskinfo->num_input_files(0),
      taskProcedure->taskinfo->level(),
      taskProcedure->taskinfo->num_input_files(1),
      taskProcedure->taskinfo->immShiftLevel== -1 ? taskProcedure->taskinfo->level() + 1 : taskProcedure->taskinfo->immShiftLevel,
      static_cast<long long>(taskProcedure->total_bytes));

  mutexS_.Lock(); //读log number以及下面对于tasktype == tAdjustmentFlush设置父亲range的smallest和largest时读兄弟的range需要持有S锁

  TaskInfo::TaskType tasktype = taskProcedure->taskinfo->tasktype();
  VersionEdit* vEdit = taskProcedure->taskinfo->edit();
  int level = taskProcedure->taskinfo->level();
  //对于Imm下移任务，vEdit记录LogNumber(比logfile_number小的log文件将在DeleteObsoleteFiles函数中被删除)
  //在恢复时调用本函数时，无需设置，因为imm是从log中恢复的，logfile_number_还未设置还是0,这样若恢复时异常退出(如断电),会重新将所有的用户log日志进行恢复
  if( taskProcedure->taskinfo->isMemTask() && !isRecoverProcess) {
    vEdit->SetPrevLogNumber(0);
    vEdit->SetLogNumber(logfile_number_); //需持有S锁
  }
  //对于tAdjustmentFlush和tDataShift任务将inputs_[0]加入修改的集合中, tAdjustmentFlush任务(范围只需缩小,放大过程在被flush时进行)
  if(tasktype == TaskInfo::tAdjustmentFlush || tasktype == TaskInfo::tDataShift) {
    RangeInfoForEdit rangeEdit;
    assert(taskProcedure->taskinfo->inputs(0).size() == 1);
    if(tasktype == TaskInfo::tDataShift) {
      rangeEdit.Set_smallest(taskProcedure->taskinfo->input(0,0)->smallest); //用于定位之前的range,old_smallest,largest不设置分别表示smallset,largest表示未变
    } else {
      //需已经持有S锁因为需要访问兄弟节点
      //对Ajustment-flush的任务设置rangeEdit的smallest,可能设置 old_smallest, largest
      setKeysForAdjust(rangeEdit, taskProcedure);
    }
    //需已经持有S锁,因为需要分配file_number, 设置剩下的所有变量
    uint64_t file_number = versions_->NewFileNumber();  //return next_file_number_++
    rangeEdit.Set_file_number(file_number);
    rangeEdit.Set_appendTimes(0);
    rangeEdit.Set_index_offset_end(config::fileCommonSize);
    rangeEdit.Set_index_offset(config::fileCommonSize);
    rangeEdit.Set_offset1(0);
    rangeEdit.Set_holeSize(config::fileCommonSize);
    rangeEdit.Set_usePercent(0);

    vEdit->AddModifyRange(level , rangeEdit);
  }
  mutexS_.Unlock();
  //对于type_ == tCombineFlush ||  tCombineShift || == tSplit || tGarbageCollection操作，inputs_[0]的记录的range插入edit的deleted ranges集合
  //对于有数据的range flush到非稳定层上，inputs_[1]中满的range插入edit的deleted ranges集合(与MakeInputIterator 和 SetApplyinfo函数一致)
  taskProcedure->taskinfo->AddInputDeletions(vEdit);

  //在DoMainDiskJob中进行磁盘操作而被修改获得的或新生成的range(磁盘上的shift操作也会新生成)插入VersionEdit的modified ranges集合或new ranges集合
  const std::vector<RangeInfoForEdit>*modifiedOutputs =  taskProcedure->getModifiedOutputs();
  const std::vector<RangeInfoForEdit>*newedOutputs =  taskProcedure->getNewedOutputs();
  for (size_t i = 0; i < modifiedOutputs->size(); i++) {
    vEdit->AddModifyRange(level + 1, (*modifiedOutputs)[i]);
  }
  for (size_t i = 0; i < newedOutputs->size(); i++) {
    if(tasktype == TaskInfo::tSplit || tasktype == TaskInfo::tGarbageCollection)
      vEdit->AddNewRange(level, (*newedOutputs)[i]);
    else if(taskProcedure->taskinfo->isMemTask() && tasktype == TaskInfo::tCombineShift) {
      vEdit->AddNewRange(taskProcedure->taskinfo->immShiftLevel, (*newedOutputs)[i]);
    }
    else
      vEdit->AddNewRange(level + 1, (*newedOutputs)[i]);
  }

}

//需已经持有S锁
//对Ajustment-flush的任务的父节点设置rangeEdit的smallest(必设置),old_smallest, largest.(参数taskProcedure不变)
void DBImpl::setKeysForAdjust(RangeInfoForEdit& rangeEdit, TaskProcedureState* taskProcedure) {
  //对于tAdjustmentFlush任务, 设置父节点使得孩子数小于等于周边(只需调小,放大过程在被flush时进行)
  Version* current = versions_->current();
  int level = taskProcedure->taskinfo->level();
  std::vector<RangeMetaData*>(&inputs)[2] = taskProcedure->taskinfo->inputs();

  const std::vector<RangeMetaData*> (&ranges) [config::kNumLevels] = current->ranges();
  int parentIdx = FindRange(internal_comparator_,  ranges[level], inputs[0][0]->smallest.Encode());
  bool hasElderBrother = false, hasYoungerBrother = false;
  std::vector<RangeMetaData*> elderBrotherChilds, youngerBrotherChilds;
  assert(parentIdx < ranges[level].size() && \
         internal_comparator_.Compare(ranges[level][parentIdx]->smallest, inputs[0][0]->smallest) == 0 );
  //获取兄弟节点的孩子信息
  if(parentIdx > 0) {
    hasElderBrother = true;
    RangeMetaData* elderBrother = ranges[level][parentIdx - 1]; //有可能其他线程已经对elderBrother加了R锁进行操作(读取到的信息可能短时间后过时)
    current->GetOverlappingInputs( level + 1,  &elderBrother->smallest, &elderBrother->largest, &elderBrotherChilds);
  }
  if(parentIdx < ranges[level].size() -1) {
    hasYoungerBrother = true;
    RangeMetaData* youngerBrother = ranges[level][parentIdx + 1]; //有可能其他线程已经对youngerBrother加了R锁进行操作
    current->GetOverlappingInputs( level + 1,  &youngerBrother->smallest, &youngerBrother->largest, &youngerBrotherChilds);
  }
  int childNum = inputs[1].size(), childNumA = elderBrotherChilds.size(), childNumC = youngerBrotherChilds.size();
  //若本节点的孩子数已经<=兄弟节点(或无某个兄弟,孩子数又小于另一个兄弟节点) 或 childNum <= levelTimes/2 则无需调整范围(缩小范围)
  if((childNum <= childNumA || !hasElderBrother) && (childNum <= childNumC || !hasYoungerBrother) || childNum <= config::levelTimes/2) {
    rangeEdit.Set_smallest(inputs[0][0]->smallest); //用于定位之前的range, old_smallest, largest不设置分别表示smallset和largest表示未变
  }
  else if ( hasElderBrother && childNum > childNumA && hasYoungerBrother && childNum > childNumC) {
    bool smallestSet = false;
    int leastChildNumA = (childNumA + childNum + childNumC) / 3;
    int leastChildNumC = leastChildNumA;
    if((childNumA + childNum + childNumC)%3 == 1) {
      leastChildNumA++;
    }
    if((childNumA + childNum + childNumC)%3 == 2){
       leastChildNumA++;
       leastChildNumC++;
    }
    if(childNumA < leastChildNumA){
       int leftReudceNum = leastChildNumA - childNumA; //左边减少的孩子数
       //mid + 1(因为mid是向下取整的)
       std::string leftString = user_comparator()->MiddleString( inputs[1][leftReudceNum - 1]->largest.user_key(), \
                                                                 inputs[1][leftReudceNum]->smallest.user_key(), true);
       InternalKey leftKey(leftString, kMaxSequenceNumber, kTypeValue);
       smallestSet = true;
       rangeEdit.Set_smallest(leftKey);
       rangeEdit.Set_old_smallest(inputs[0][0]->smallest); //若smallest与inputs[0][0]发生了变化,则可能设置
    }
    if(childNumC < leastChildNumC) {
       int rightReudceNum = leastChildNumC - childNumC; //右边减少的孩子数
       assert(childNum - 1 - rightReudceNum >= 0);
       std::string rightString = user_comparator()->MiddleString( inputs[1][childNum - 1 - rightReudceNum]->largest.user_key(), \
                                                                  inputs[1][childNum - rightReudceNum]->smallest.user_key() );
       InternalKey rightKey(rightString, kMaxSequenceNumber, kTypeValue);
       rangeEdit.Set_largest(rightKey);
    }
    if(!smallestSet) {  //前面可能并未设置smallest，即左边范围并没发生改变(如从左到右孩子分别为13, 17, 9)
      rangeEdit.Set_smallest(inputs[0][0]->smallest);
    }
  }
  else if(hasElderBrother && childNum > childNumA ) {
    int leastChildNumA = (childNumA + childNum) / 2 + (childNumA + childNum ) % 2;
    int leftReudceNum = leastChildNumA - childNumA; //左边减少的孩子数
    //mid + 1
    std::string leftString = user_comparator()->MiddleString( inputs[1][leftReudceNum - 1]->largest.user_key(), \
                                                              inputs[1][leftReudceNum]->smallest.user_key(), true);
    InternalKey leftKey(leftString, kMaxSequenceNumber, kTypeValue);
    rangeEdit.Set_smallest(leftKey);
    rangeEdit.Set_old_smallest(inputs[0][0]->smallest); //若smallest与inputs[0][0]发生了变化,则可能设置
  }
  else if(hasYoungerBrother && childNum > childNumC) {
    int leastChildNumC = (childNumC + childNum) / 2 + (childNumC + childNum ) % 2;
    int rightReudceNum = leastChildNumC - childNumC; //右边减少的孩子数
    std::string rightString = user_comparator()->MiddleString( inputs[1][childNum - 1 - rightReudceNum]->largest.user_key(), \
                                                               inputs[1][childNum - rightReudceNum]->smallest.user_key() );
    InternalKey rightKey(rightString, kMaxSequenceNumber, kTypeValue);
    rangeEdit.Set_largest(rightKey);
    rangeEdit.Set_smallest(inputs[0][0]->smallest);
  }
  else {
    assert(0);
  }
}



namespace {
struct IterState {
  port::Mutex* mu;
  Version* version;
  MemTable* mem;
  MemTable* imm;
};

//迭代器析构时自动调用的函数，解引用memTable, immutable memtable(若有的话), version
//ok
static void CleanupIteratorState(void* arg1, void* arg2) {//第二个参数没有逻辑作用，只是为了满足某一函数指针
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != NULL) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}
}  // namespace

//将数据库最新的last_sequence_赋值给latest_snapshot;  *seed = ++seed_;
// 将memTable, immutable memtable(若有的话), version合成一个迭代器返回
// ok
Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  IterState* cleanup = new IterState;
  mutexS_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != NULL) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  cleanup->mu = &mutexS_;
  cleanup->mem = mem_;
  cleanup->imm = imm_;
  cleanup->version = versions_->current();
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL); //注册当迭代器析构时调用的函数CleanupIteratorState(cleanup,NULL)

  *seed = ++seed_;
  mutexS_.Unlock();
  return internal_iter;
}

//ok
Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}


//若ReadOption没有指定snapshot（快照，一个数字），读小于该snapshot的内容，先读memtable 若没找到 若有immutable  memtable，若没找到 读current_版本下的（通过Cache filter等）文件。
//读MemTable（memtable 和 imutable memtable）和current_版本下的文件时 不需要占用mutexS_。
//ok
Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutexS_);
  SequenceNumber snapshot;
  if (options.snapshot != NULL) {//默认为NULL(用户可设置)
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();//整个数据库最新的sequence number
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != NULL) imm->Ref();
  current->Ref();
  bool have_stat_update = false;
  Version::GetStats stats;      

  // Unlock while reading from files and memtables
  {
    mutexS_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    // Memtable::Get和Version::Get不需要额外同步!
    LookupKey lkey(key, snapshot);//snapshot(SequenceNumber)大的反而小(后面插入的不会被搜索到,因为后面的Get函数使用迭代器首先找到一个>=iternal key的第一个记录)
    if (mem->Get(lkey, value, &s)) {//读取成功返回ture
      // Done
    } else if (imm != NULL && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    //重新申请持有mutexS_
    mutexS_.Lock();
  }

  //正确性论证:若current为current_,那么current中将要记录的RangeMetaData状态不会改变,正确;
    //若这里的current非current_,current中记录的RangeMetaData在最新版本中状态可能变化了（甚至不存在）,没关系，因为选取任务是基于current_的,current中的值没影响
  if (have_stat_update && current->UpdateStats(stats)) {//通过读来指导任务获取
    env_->Enq_task(BGWork, this, wakeMutexForReleaseDB(), wakeCondForReleaseDB() ); //添加任务
  }
  mem->Unref();
  if (imm != NULL) imm->Unref();
  current->Unref();
  return s;
}


//创建的迭代器从用户的角度应该有的迭代器的功能（包括对删除或者更新的记录的正确的查找; 快照即seqeunce number功能）
//实现了读的优化，(迭代器平均遍历1MB的数据调用一次RecordReadSample以指导后台线程选取任务,再将任务放入后台队列中,可能启动后台线程进行compaction)
//ok
Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;

  //将数据库最新的last_sequence_赋值给latest_snapshot
  // *seed = ++seed_;(产生随机数的种子)
  // 将memTable, immutable memtable(若有的话), version合成一个迭代器返回
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  //创建的迭代器从用户的角度应该有的迭代器的功能（包括对删除或者更新的记录的正确的查找; 快照即seqeunce number功能）
  return NewDBIterator(
      this, user_comparator(), iter,
      (options.snapshot != NULL //默认为NULL
       ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
       : latest_snapshot),
      seed);
}

//DBIter类（迭代器)调用，平均每解析1MB数据调用一次，已实现读的优化(再调用再将任务放入后台队列中,可能启动后台线程选取任务)
void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutexS_); //现持有mutexS_，因为调用下面几个函数都需要持有mutexS_
  if (versions_->current()->RecordReadSample(key)) {
    env_->Enq_task(BGWork, this, wakeMutexForReleaseDB(), wakeCondForReleaseDB() ); //添加任务
  } 
}//释放mutexS_
    
//向snapshots_（SnapshotList）插入一个以当前的last_sequence_值参数的SnapshotImpl, 并返回这个SnapshotImpl*(ReleaseSnapshot的参数)
//ok
const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutexS_);
  return snapshots_.New(versions_->LastSequence());
}

//在snapshots_（SnapshotList）删除一个结点
//ok
void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  MutexLock l(&mutexS_);
  snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
// 调用DB中纯虚函数的实现
// ok
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

// 调用DB中纯虚函数的实现
// ok
Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

//通过my_batch信息(可能结合writers_（deque），若后面还有的话），以写日志，写memtable，当然可能触发新的compaction操作
//将修改last_sequence_
//caixin:  NULL batch means just wait for earlier writes to be done即前面的所有写请求（如多线程导致的）全部完成
//ok
Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
  Writer w(&writers_lock_);// 没有持有writers_lock_,Information kept for every waiting writer，是std::deque的一个元素
  w.batch = my_batch;
  w.sync = options.sync;
  w.done = false;

   //申请持有mutex_
  writers_lock_.Lock();
  writers_.push_back(&w);//插入的是指针
  while (!w.done && &w != writers_.front()) {//相等才会跳出该循环，即其他先提交的writer请求写完才执行本函数
    w.cv.Wait();//wait期间将释放mutex_,被signal通知后,申请mutex_,若成功,awake
  }
  if (w.done) { //可能已经被前面的WriterBatch的线程放在一块处理完成了
    writers_lock_.Unlock(); 
    return w.status;
  }

  mutexS_.Lock();

  // May temporarily unlock and wait.
  //memtable有空间(<=4MB)且level0的文件个数小于8,则立即退出函数，否则 多种情况
  //比如memtable满了，将新建一个新的当前的log文件和mem_以前的被转换为imm_,并启动一个后台线程
  Status status = MakeRoomForWrite(my_batch == NULL);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && my_batch != NULL) {  // NULL batch is for compactions
    WriteBatch* updates = BuildBatchGroup(&last_writer);// updates可能为writers_(deque)中多个Batch(若有的话)的综合(没有从writers_ pop)
    WriteBatchInternal::SetSequence(updates, last_sequence + 1);//设置 the seqeunce number for the start of this batch.
    last_sequence += WriteBatchInternal::Count(updates);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    //!这里是利用写队列和条件变量同步的，使同时只有一个线程在执行下面操作且只有本函数才写log文件和mem_， 而读log只在数据库刚打开的时候，读mem_线程安全不需要同>步，(这样后台进程可以同时进行compaction操作，提高了并行度)
    {
      mutexS_.Unlock(); 
      writers_lock_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(updates));//写先log，注意这里的
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        //遍历updates中的所有记录以写入MemTable,其sequence Number从b中保存的开始，每一条记录+1)
        status = WriteBatchInternal::InsertInto(updates, mem_);
      }
      writers_lock_.Lock();//重新占用两种锁(先对writers_lock_加锁,以防死锁)
      mutexS_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (updates == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);//设置VersionSet的last_sequence_
  }

  mutexS_.Unlock();

  //从writers_中pop掉已经处理了的请求
  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true; //通知将要被唤醒的线程，该WriteBatch已经完成了
      ready->cv.Signal();//注意每pop_front一个(只要不是最开头的那个)就要调用ready->cv.Signal()一次以唤醒等待的线程,注意每次使用的条件变量是不同的,以按序唤醒等>待线程，不至于混乱
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal(); //唤醒下一个WriteBatch对应的线程(若有的话)
  }

  writers_lock_.Unlock();

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
// 输出1：返回的是writers_(deque)中前面多个(也可能只有一个)WriteBatch按序append的生成的WriteBach的地址
// 输出2：*last_writer保存的是从writers_（deque)最后一个append到result的WriteBach*
// 说明：append的个数受max_size的影响，writers_.front()中WriteBatch的size<=128KB, 则max_size为其size+128KB(即最多append128KB)
// 若其size >128KB,按序append writers_中的WriteBatch,其max_size为2MB(即最多append 2MB-size)若第一个超过两2MB或加上第二个超过2MB则只接受第一个
// 上述两种情况若第一个的sync为false 而遇到某一个sync为ture则停止，不进行append 或者writes_（deque）已经没有了
// ok
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != NULL);

  size_t size = WriteBatchInternal::ByteSize(first->batch);//该类全是静态方法

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;//1MB
  if (size <= (128<<10)) {
    max_size = size + (128<<10); //<=128KB *2 即256KB
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {//从第二个开始
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != NULL) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *reuslt
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;//指针赋值,tmp_batch_ 初始时header（12个字节)全为0的WriteBach对象
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);//将*(first->batch)的所有记录append到*result
      }
      WriteBatchInternal::Append(result, w->batch);   //将*(w->batch)的所有记录append到*result
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
// force为强制的意思,若force为true，不管Memtable是否满，都将其转换为Immutable Memtable并新建一个日志文件,若Immutable memtable存在则等待其先被处理再进行;
// 调用时当写请求不需要写记录时是force为true，否则为false(正常情况)
// 检查是否有写的空间，若没有足够空间将会唤醒后台线程进行处理或者选择等待(等待后台compaction线程将Immutable Memtable移除而腾出了空间然后唤醒本线程)
// ok
Status DBImpl::MakeRoomForWrite(bool force) {
  mutexS_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force; //本次写是否允许dellay,因为force为true不写记录时调用，所以为allow_delay为false
  size_t memSize = 0;
  Status s;
  while (true) {
    memSize = mem_->ApproximateMemoryUsage();
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (!force && //若force为ture不执行
               (memSize <= options_.write_buffer_size)) {//当前的memtable还有空间,不做任何等待,跳出循环

      if(imm_!=NULL && !isDoingImmTask_ && allow_delay && memSize >= (options_.write_buffer_size>>1) ) { //isDoingImmTask_的作用是不要Imm处理完了但是Mem还没满，影响性能(虽然还是可能引起高负载下性能的波动)
        mutexS_.Unlock();
        env_->SleepForMicroseconds(config::yellow_sleep_us);
        allow_delay = false;  // Do not delay a single write more than once
        mutexS_.Lock();
        continue;
      }
      // There is room in current memtable
      break;
    } else if (imm_ != NULL) { //force为ture或若memtable已经满，而immutable memtable还没有被compacted，则wait直到被唤醒(将执行compaction)
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      //等待后台线程(本函数是生成imm_的唯一途径,生成后会显式创建后台线程执行操作,故一定有后台线程在执行操作)将Immutable Memtable移除后会唤醒
      condForImmDownOrErr_.Wait();
    } else {    //若force为false则memtable已经满,若force为true则Memtable不需满;没有immutable memtable,此时将memtable转换为immutable (将原来的log关闭,新建一个log文件，新建一个memtable，调用MaybeSheduleCompaction：一定会触发新建一个后台线程执行compaction)
      // Attempt to switch to a new memtable and trigger compaction of old
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber();
      WritableFile* lfile = NULL;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }
      delete log_;
      delete logfile_; //关闭文件
      logfile_ = lfile;

      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;   // Do not force another compaction if have room
      //需要将新的任务放到env中（会自动唤醒至少一个线程）使一个后台线程进行操作
      //这是两个调用本函数的地方之一(还有一个在后台线程中发现PickTask成功且数据库未关闭或PickTask失败但需要插入任务, 调用本函数),需要满足数据库还未关闭因为MakeRoomForWrite是用户现线程调用的,所以数据库用户需要保证此逻辑即不可以一边关闭数据库一边插入任务，否则后台线程可能得到的是已经关闭的数据库的任务
      env_->Enq_task(BGWork, this, wakeMutexForReleaseDB(), wakeCondForReleaseDB() );
      //下面的循环将执行：若没发生错误,memtable(刚刚新建的)一定有空间，直接退出函数
    }
  }
  return s;
}


//property下面三个格式选一 "branchdb.num-files-at-level<N>" , "branchdb.stats" , "branchdb.ranges"
//ok
bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutexS_);
  Slice in = property;
  Slice prefix("branchdb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "%d",
               versions_->NumLevelFiles(static_cast<int>(level)));//返回第level层 current_记录的文件个数
      *value = buf;
      return true;
    }
  } else if (in == "stats") { //输出stats_(记录每层task操作的花费)
    char buf[200];
    snprintf(buf, sizeof(buf),
             "                               Compactions\n"
             "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
             "--------------------------------------------------\n"
             );
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        snprintf(
            buf, sizeof(buf),
            "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
            level,
            files,
            versions_->NumLevelBytes(level) / 1048576.0,
            stats_[level].micros / 1e6,
            stats_[level].bytes_read / 1048576.0,
            stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "ranges") {
    *value = versions_->current()->DebugString();// Return a human readable string that describes this version's contents(全部的level的全部文件的描述信息).
    return true;
  }

  return false;
}


// Default implementations of convenience methods that subclasses of DB
// can call if they wish
// ok
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);//Sequence number 在Write函数中设置

  return Write(opt, &batch);
}

//ok
Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() { }

//ok
Status DB::Open(const Options& options, const std::string& dbname,
                DB** dbptr) {

  *dbptr = NULL;

  DBImpl* impl = new DBImpl(options, dbname); //初始相应的成员变量
  impl->mutexS_.Lock();

  VersionEdit edit; //默认构造函数
  VersionEdit* versionEdits[1];
  versionEdits[0] = &edit;
  BySmallest bysmallest(impl->internal_comparator_);
  ApplyInfo applyinfo(bysmallest); //采取默认设置

  //见函数注释
  Status s = impl->Recover(); // Handles create_if_missing, error_if_exists,将设置manifest_file_number_

  if (s.ok()) {
    uint64_t new_log_number = impl->versions_->NewFileNumber(); //即next_file_number_, 返回后++, 若刚打开数据库这里为3
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile);
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);  //Recover时，new_log_number自己及以后会恢复
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      //将会将快照写入新的MANFIEST文件并将其文件名写入CURRENT文件
      s = impl->versions_->LogAndApply(versionEdits, &applyinfo, 1, &impl->mutexS_, NULL, NULL, false);
    }
    if (s.ok()) {
      impl->DeleteObsoleteFiles(true, false); //删除多余文件，可以删除多于log文件,这里是删除原MANIFIFEST文件的唯一地方,SStable需要从Table Cache中驱除(无法识别的文件不删除)
      impl->env_->Enq_task(impl->BGWork, impl, impl->wakeMutexForReleaseDB(), impl->wakeCondForReleaseDB() ); //唤醒后台线程执行任务
    }
  }
  impl->mutexS_.Unlock();
  if (s.ok()) {
    *dbptr = impl;
  } else {
    delete impl;
    impl = NULL;
  }
  return s;
}

Snapshot::~Snapshot() {
}

//销毁数据库：
//先占用LOCK文件的独占性写锁：数据库没有进程在访问（如,访问了但已经关闭了）
//然后删除dbname目录下的所有文件，然后删除每一个文件（LOCK文件最后删除，删除LOCK文件后删除数据库目录本身）
//ok
Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);  //将dbname目录下的每一个文件名push_back至*filenames
  if (filenames.empty()) {
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);//生成LOCK文件名，如sunLevel/LOCK
  Status result = env->LockFile(lockname, &lock); //对LOCK文件加独占性写锁：数据库没有进程在访问（如访问了但已经关闭了）
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->DeleteFile(dbname + "/" + filenames[i]);//即unlink
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace branchdb
