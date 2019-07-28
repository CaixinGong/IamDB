// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/log_reader.h"

#include <stdio.h>
#include "branchdb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace branchdb {
namespace log {

Reader::Reporter::~Reporter() {
}

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(), //Slice的默认构造函数，在ReadPhysicalRecord会重新关联backing_store,而长度为读取的当前block的长度(最后一个可能不满)。
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset) {
}

Reader::~Reader() {
  delete[] backing_store_;
}

bool Reader::SkipToInitialBlock() {//seek到"块的起始地址"
  size_t offset_in_block = initial_offset_ % kBlockSize;//块内偏移量
  uint64_t block_start_location = initial_offset_ - offset_in_block;//所在块的首地址

  // Don't search a block if we'd be in the trailer(尾部)
  if (offset_in_block > kBlockSize - 6) {
    offset_in_block = 0;
    block_start_location += kBlockSize;
  }

  end_of_buffer_offset_ = block_start_location; //ReadPhysicalRecord中会被修改为当前block的末尾偏移量

  // Skip to start of first block that can contain the initial record
  if (block_start_location > 0) {
    Status skip_status = file_->Skip(block_start_location);//从SEEK_CUR开始的偏移
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);//将错误消息传给reporter对象，所以reporter_赋值了才有现象（可以不赋值）
      return false;
    }
  }

  return true;
}

bool Reader::ReadRecord(Slice* record, std::string* scratch) {//读取一条完整的记录，只包含有效数据(不包含head部分),
                                                          //若是非FULL类型的，将record的内容与scratch关联，FULL类型则和backing_restore_关联
														  //这么做的原因是FULL类型的value在log文件是连续存储的，读入内存后可以直接使用。
if (last_record_offset_ < initial_offset_) {//这里只在一开始的时候并且初始的initial_offset_非0才会执行;
    if (!SkipToInitialBlock()) {            //last_record_offset_在读取一条完整的record后会发生变化
      return false;
    }
  }
  scratch->clear();
  record->clear();
  bool in_fragmented_record = false;
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  uint64_t prospective_record_offset = 0;//看上面注释：要读取得记录的偏移量,prospective(未来的，预期的,将要的）

  Slice fragment;
  while (true) {
    uint64_t physical_record_offset = end_of_buffer_offset_ - buffer_.size();//当前要读取的"可能不完整记录"的偏移量
    const unsigned int record_type = ReadPhysicalRecord(&fragment);//fragment中只包含一条record的有效信息（不包括header）,返回type
    switch (record_type) {                                        //frament也是间接关联到backing_restore_(frament--->buffer_--->backing_restore_)
      case kFullType://若是full类型则一般用不到scratch
        if (in_fragmented_record) {///默认为false,当进入kFirstType时，改为true,
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (scratch->empty()) {//这里现在版本不执行，见上注释; 是否为空,返回bool
            in_fragmented_record = false;
          } else {
            ReportCorruption(scratch->size(), "partial record without end(1)");//这是上一条record出现错误，执行scratch->clear()抛弃这些内容，本条记录正确
          }
        }
        prospective_record_offset = physical_record_offset;//为一条"完整记录"偏移量
        scratch->clear();
        *record = fragment;//若是FUll类型，则record内容间接关联到backing_restore_
        last_record_offset_ = prospective_record_offset;
        return true;

      case kFirstType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (scratch->empty()) {//这里现在版本不执行，见上注释; 是否为空,返回bool
            in_fragmented_record = false;
          } else {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        prospective_record_offset = physical_record_offset;
        scratch->assign(fragment.data(), fragment.size());//若是非FULL类型的，则将record的内容与scratch关联
        in_fragmented_record = true;
        break;

      case kMiddleType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());//若是非FULL类型的，则将record的内容与scratch关联
        }
        break;

      case kLastType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);//若是非FULL类型的，则将record的内容与scratch关联
          last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;

      case kEof://类内定义的nameless enum，用户不可见，坏块的一种
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "partial record without end(3)");
          scratch->clear();
        }
        return false;

      case kBadRecord://类定义内的, 用户不可见，坏块的一种
			//当ReadPhysicalRecord读的记录在initial_block_之前的记录返回kBadRecord,以过滤掉这些这记录
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break; //跳出switch语句继续执行while循环，以过滤掉这些这记录

      default: {//其他类型的坏块
        char buf[40];
        snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

uint64_t Reader::LastRecordOffset() {
  return last_record_offset_;
}

void Reader::ReportCorruption(size_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(size_t bytes, const Status& reason) {
  if (reporter_ != NULL &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(bytes, reason);
  }
}

unsigned int Reader::ReadPhysicalRecord(Slice* result) {//result中只包含一条record的有效信息（不包括header）,返回type
  while (true) {
    if (buffer_.size() < kHeaderSize) {//注意如果buffer_中还有数据未处理则不实际从文件中读取,而是直接从buffer_空间中解析一条新的record并返回
      if (!eof_) {
        // Last read was a full read, so this is a trailer to skip,或者第一次执行这，buffer_还未关联backing_store_
        buffer_.clear();
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);//读取整个块填充backing_store_，并将buffer_的内容关联backing_store_
        end_of_buffer_offset_ += buffer_.size();                           //长度关联到读取的长度， 当读取内容小于n时，若到达文件末尾正常退出，否则返回错误消息
        if (!status.ok()) {//读错误:只读取了部分数据（存在更多的数据）
          buffer_.clear();
          ReportDrop(kBlockSize, status);
          eof_ = true;
          return kEof;
        } else if (buffer_.size() < kBlockSize) {//读取正确，但是到达文件结尾，未读满kBlockSize
          eof_ = true;
        }
        continue;//读取正确，且读取了整个kBlockSize,继续循环以执行下面的parse the header
      } else if (buffer_.size() == 0) {
        // End of file
        return kEof;
      } else {
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "truncated record at end of file");
        return kEof;
      }
    }

    // Parse the header
    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;//length的低8位
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;//length的高8位
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);
    if (kHeaderSize + length > buffer_.size()) {//因为存储的length为本block中的有效数据的长度，故若该式成立为逻辑坏块
      size_t drop_size = buffer_.size();
      buffer_.clear();
      ReportCorruption(drop_size, "bad record length");
      return kBadRecord;
    }

    if (type == kZeroType && length == 0) {// Zero is reserved for preallocated files
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }

    buffer_.remove_prefix(kHeaderSize + length);   // buffer_为Slice对象, Drop the first "n"  bytes from this slice.

    // Skip physical record that started before initial_offset_
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <       //buffer_.size()已减少，因为上一步
        initial_offset_) {
      result->clear();
      return kBadRecord;
    }

    *result = Slice(header + kHeaderSize, length);    //只读取有效信息（不包括header）
    return type;
  }
}

}  // namespace log
}  // namespace branchdb
