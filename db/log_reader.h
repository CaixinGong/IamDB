// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_BRANCHDB_DB_LOG_READER_H_
#define STORAGE_BRANCHDB_DB_LOG_READER_H_

#include <stdint.h>

#include "db/log_format.h"
#include "branchdb/slice.h"
#include "branchdb/status.h"

namespace branchdb {

class SequentialFile;

namespace log {

class Reader {
 public:
  // Interface for reporting errors.
  class Reporter {//有两个类实现了接口，都将corruption函数的错误进行输出，或进行保存未进行实际解决(因为单机无法解决，是允许出现坏块的，如突掉电写到一半）
   public:
    virtual ~Reporter();

    // Some corruption was detected.  "size" is the approximate number
    // of bytes dropped due to the corruption.
    virtual void Corruption(size_t bytes, const Status& status) = 0; //都将corruption函数的错误进行输出，或进行保存未进行实际解决
  };

  // Create a reader that will return log records from "*file".
  // "*file" must remain live while this Reader is in use.
  //
  // If "reporter" is non-NULL, it is notified whenever some data is
  // dropped due to a detected corruption.  "*reporter" must remain
  // live while this Reader is in use.
  //
  // If "checksum" is true, verify checksums if available.
  //
  // The Reader will start reading at the first record located at physical
  // position >= initial_offset within the file.注意这条记录
  Reader(SequentialFile* file, Reporter* reporter, bool checksum,//SequentialFile提供了Read(),Skip(),两个纯虚函数的抽象基类
         uint64_t initial_offset);

  ~Reader();

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of the input.  May use
  // "*scratch" as temporary storage.  The contents filled in *record
  // will only be valid until the next mutating operation on this
  // reader or the next mutation to *scratch.
  bool ReadRecord(Slice* record, std::string* scratch);//读取一条"完整的"记录，只包含有效数据(不包含head部分),
                                                       //则若是非FULL类型的，将record的内容与scratch关联，FULL类型则和backing_restore_关联

  // Returns the physical offset of the last record returned by ReadRecord.
  //
  // Undefined before the first call to ReadRecord.
  uint64_t LastRecordOffset();

 private:
  SequentialFile* const file_;//4个常量指针
  Reporter* const reporter_;
  bool const checksum_;
  char* const backing_store_;//作为buffer_的内容存放的空间,一般为存储整个block的内容
  Slice buffer_;//读取log文件的内容, 在ReadPhysicalRecord会重新关联backing_store,而长度为读取的当前block的长度(最后一个可能不满)。
   
  bool eof_;   // Last Read() indicated EOF by returning < kBlockSize

  // Offset of the last record returned by ReadRecord.
  uint64_t last_record_offset_;
  // Offset of the first location past the end of buffer_.
  uint64_t end_of_buffer_offset_;//看英文助手

  // Offset at which to start looking for the first record to return
  uint64_t const initial_offset_;

  // Extend record types with the following special values
  enum {//和static const 的作用类似，但是enum一定不分配内存空间
    kEof = kMaxRecordType + 1,//5
    // Returned whenever we find an invalid physical record.
    // Currently there are three situations in which this happens:
    // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
    // * The record is a 0-length record (No drop is reported)
    // * The record is below constructor's initial_offset (No drop is reported)
    kBadRecord = kMaxRecordType + 2
  };

  // Skips all blocks that are completely before "initial_offset_".
  //
  // Returns true on success. Handles reporting.
  bool SkipToInitialBlock();

  // Return type, or one of the preceding special values
  unsigned int ReadPhysicalRecord(Slice* result);

  // Reports dropped bytes to the reporter.(bytes为错误数据（坏块）的字节数)
  // buffer_ must be updated to remove the dropped bytes prior to invocation(调用，告知).
  void ReportCorruption(size_t bytes, const char* reason);
  void ReportDrop(size_t bytes, const Status& reason);

  // No copying allowed
  Reader(const Reader&);
  void operator=(const Reader&);
};

}  // namespace log
}  // namespace branchdb

#endif  // STORAGE_BRANCHDB_DB_LOG_READER_H_
