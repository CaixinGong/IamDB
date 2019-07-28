// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_BRANCHDB_DB_LOG_WRITER_H_
#define STORAGE_BRANCHDB_DB_LOG_WRITER_H_

#include <stdint.h>
#include "db/log_format.h"
#include "branchdb/slice.h"
#include "branchdb/status.h"

namespace branchdb {

class WritableFile;

namespace log {

class Writer {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  explicit Writer(WritableFile* dest);
  ~Writer();

  Status AddRecord(const Slice& slice);//唯一接口,只允许append操作

 private:
  WritableFile* dest_;//只有以下3个成员变量,其中，WritableFile 类型是一个纯虚函数 
  int block_offset_;       // Current offset in block

  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  uint32_t type_crc_[kMaxRecordType + 1];//5种(//第0种代表: Zero is reserved for preallocated files)

  Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);//确保内容从用户缓冲空间发送给内核空间，并且修改了this.block_offset_, 供AddRecord调用

  // No copying allowed
  Writer(const Writer&);
  void operator=(const Writer&);
};

}  // namespace log
}  // namespace branchdb

#endif  // STORAGE_BRANCHDB_DB_LOG_WRITER_H_
