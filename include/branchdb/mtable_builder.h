// MtableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a MtableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same MtableBuilder must use
// external synchronization.

#ifndef STORAGE_BRANCHDB_INCLUDE_MTABLE_BUILDER_H_
#define STORAGE_BRANCHDB_INCLUDE_MTABLE_BUILDER_H_

#include <stdint.h>
#include "branchdb/options.h"
#include "branchdb/status.h"

namespace branchdb {

class BlockBuilder;
class BlockHandle;
class WritableFile;
class PwritableFile;

class MtableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish().
  // offset1(data_offset)表示这次数据部分要写的末尾, offset2(index_offset)表示这次索引部分写完会达到的地方, holeSize表示文件内部洞的大小, option.preAllocateSpace为true则对于新的MSStable进行预留空间, 当offset1, offset2同时为0时表示本文件只存储一个sorted string，不会预留空间且文件中间没有洞
  // r为返回值,r.ok()为true表示预留空间是否成功
  MtableBuilder(const Options& options, PwritableFile* file, uint64_t offset1, uint64_t offset2, uint64_t holeSize, Status& r);

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~MtableBuilder();

  // Change the options used by this builder.  Note: only some of the
  // option fields can be changed after construction.  If a field is
  // not allowed to change dynamically and its value in the structure
  // passed to the constructor is different from its value in the
  // structure passed to this method, this method will return an error
  // without changing any fields.
  // 本函数没有经过测试，应该有问题
  Status ChangeOptions(const Options& options);

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value);

  // Advanced operation: flush any buffered key/value pairs to file.
  // Can be used to ensure that two adjacent entries never live in
  // the same data block.  Most clients should not need to use this method.
  // REQUIRES: Finish(), Abandon() have not been called
  // 即将当前内存中的数据以一个data block打包写入.sst文件中，不用等到达到data block达到阈值（默认4KB）
  void Flush();

  // Return non-ok iff some error has been detected.
  Status status() const;

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  // 创建一个.sst文件完成，不可再add
  Status Finish();


  //返回当前已有的索引数据的偏移量, 调用了Finish或Abandon之后才能调用
  uint64_t Index_offset();

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  // 即还没有调用Finish()前调用，写入.sst文件的格式不完整，无法解析，故该.sst文件被丢弃
  void Abandon();

  // Number of calls to Add() so far.
  uint64_t NumEntries() const;

  // Size of the file generated so far. 
  // 返回的是offset1,调用了Finish或Abandon之后才能调用
  uint64_t Offset1() const;

  //供未Finish时实时调用,返回文件至今已写的大概的大小(不包含索引部分)
  uint64_t CurrentFileSize() const;

  // Size of the file generated so far. 
  // 返回的是offset1-base_offset1,调用了Finish或Abandon之后才能调用
  uint64_t FileSizeByThisBuilder(bool includeMeta = true) const;

  // 调用了Finish或Abandon之后才能调用
  uint64_t HoleSize() const;
  uint64_t IsAbandoned() const;

 private:
  enum Towhich{
    toData = 1,
    toIndex = 2   
  };
  bool ok() const { return status().ok(); }
  void WriteBlock(BlockBuilder* block, BlockHandle* handle,  Towhich towhich ); //Towhich为1表示写的是data部分，2表示写索引部分
  void WriteRawBlock(const Slice& data, CompressionType, BlockHandle* handle, Towhich towhich);

  struct Rep; //私有的类成员
  Rep* rep_;  //唯一的成员变量

  // No copying allowed
  MtableBuilder(const MtableBuilder&);
  void operator=(const MtableBuilder&);
};

}  // namespace branchdb

#endif  // STORAGE_BRANCHDB_INCLUDE_TABLE_BUILDER_H_
