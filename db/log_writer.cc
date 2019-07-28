// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/log_writer.h"

#include <stdint.h>
#include "branchdb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"
namespace branchdb {
namespace log {
Writer::Writer(WritableFile* dest)
    : dest_(dest),//本类用到dest_->Append(const Slice&) , dest_->Flush(),因为WritableFile为抽象基类(包含纯虚函数)，而且有多个类继承并实现了该函数(使用了 PosixWritableFile类实现,该对象析构时关闭对应文件)
      block_offset_(0) {
  for (int i = 0; i <= kMaxRecordType; i++) { //初始化type_crc_数组，预先计算type的校验码，减少计算校验码的开销
    char t = static_cast<char>(i);
    type_crc_[i] = crc32c::Value(&t, 1); //函数的作用为计算t[0 至(1-1)]的校验码，即计算type的校验码
  }
}

Writer::~Writer() {
}

Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();
  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
    const int leftover = kBlockSize - block_offset_;//该block剩余空间，this.block_offset_, kBlockSize在log_format.h log命名空间中
    assert(leftover >= 0);
    if (leftover < kHeaderSize) {//剩0-6个字节，则对剩余字节置零
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        assert(kHeaderSize == 7);
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;//该block还可以写 有用的data(全部value或一部分) 的空间:available
    const size_t fragment_length = (left < avail) ? left : avail;//该block将要写的value的长度

    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      type = kFullType;
    } else if (begin) {
      type = kFirstType;
    } else if (end) {
      type = kLastType;
    } else {
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length);//确保内容从用户缓冲空间发送给内核空间，并且修改了this.block_offset_
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) {//私有函数供AddRecord调用,向绑定的文件中写7+n个字符: head+ptr[0 - n-1]
                                                                         //确保内容从用户缓冲空间发送给内核空间，并且修改了this.block_offset_
  assert(n <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + n <= kBlockSize);

  // Format the header
  char buf[kHeaderSize];
  buf[4] = static_cast<char>(n & 0xff); //buf[4,5]小端法存储n，即n最大为16位的无符号整数：2^16-1即 64KB -1 
  buf[5] = static_cast<char>(n >> 8);
  buf[6] = static_cast<char>(t); // 即type

  // Compute the crc of the record type and the payload(有效载荷).
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, n);
  crc = crc32c::Mask(crc);                 // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload(有效载荷).
  Status s = dest_->Append(Slice(buf, kHeaderSize));//写记录head
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, n));//写payload
    if (s.ok()) {
      s = dest_->Flush();//确保缓冲区的内容冲洗至内核
    }
  }
  block_offset_ += kHeaderSize + n;//修改this.block_offset_
  return s;
}

}  // namespace log
}  // namespace branchdb
