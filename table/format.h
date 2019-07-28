// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_BRANCHDB_TABLE_FORMAT_H_
#define STORAGE_BRANCHDB_TABLE_FORMAT_H_

#include <string>
#include <stdint.h>
#include "branchdb/slice.h"
#include "branchdb/status.h"
#include "branchdb/mtable_builder.h"

namespace branchdb {

class Block;
class RandomAccessFile;
struct ReadOptions;

// BlockHandle is a pointer to the extent of a file that stores a data
// block or a meta block.
class BlockHandle {
 public:
  BlockHandle();  //定义见后，将两个成员变量设置为uint64_t 0

  // The offset of the block in the file.
  uint64_t offset() const { return offset_; }
  void set_offset(uint64_t offset) { offset_ = offset; }

  // The size of the stored block
  uint64_t size() const { return size_; }
  void set_size(uint64_t size) { size_ = size; }
  bool isValid() {
    return offset_ != (~static_cast<uint64_t>(0)) && size_ != (~static_cast<uint64_t>(0));
  }

  //以下两函数定义于 table/format.cc
  //将offset_ , size_的值编码为varint64，并append到*dst
  void EncodeTo(std::string* dst) const;
  //将input中的varint64的两个值 解码到offset_ , size_
  Status DecodeFrom(Slice* input);

  // Maximum encoding length of a BlockHandle
  enum { kMaxEncodedLength = 10 + 10 };

 private:
  uint64_t offset_;
  uint64_t size_;
};

// Footer encapsulates the fixed information stored at the tail
// end of every table file.
class Footer {
 public:
  Footer() { }

  // The block handle for the metaindex block of the table
  const BlockHandle& filter_handle() const { return filter_handle_; }
  //void set_metaindex_handle(const BlockHandle& h) { metaindex_handle_ = h; }
  void set_filter_handle(const BlockHandle& h) { filter_handle_ = h; }

  // The block handle for the index block of the table
  const BlockHandle& index_handle() const {
    return index_handle_;
  }
  void set_index_handle(const BlockHandle& h) {
    index_handle_ = h;
  }
  bool isValid() {
    return index_handle_.isValid();
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);

  // Encoded length of a Footer.  Note that the serialization of a
  // Footer will always occupy exactly this many bytes.  It consists
  // of two block handles and a magic number.
  enum {
    kEncodedLength = 2*BlockHandle::kMaxEncodedLength + 8
  };

 private:
  BlockHandle filter_handle_;
  BlockHandle index_handle_;
};

// kTableMagicNumber was picked by running
//    echo http://code.google.com/p/branchdb/ | sha1sum
// and taking the leading 64 bits.
static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;

// 1-byte type + 32-bit crc
static const size_t kBlockTrailerSize = 5;

struct BlockContents {
  Slice data;           // Actual contents of data
  bool cachable;        // True iff data can be cached
  bool heap_allocated;  // True iff caller should delete[] data.data()
};

// Read the block identified by "handle" from "file".  On failure
// return non-OK.  On success fill *result and return OK.
extern Status ReadBlock(RandomAccessFile* file,
                        const ReadOptions& options,
                        const BlockHandle& handle,
                        BlockContents* result);

//注意返回result中的内存是从堆中分配的
extern Status ReadBlockFromMem(const char* allData,
                 const ReadOptions& options,
                 const BlockHandle& handle,
		 uint64_t baseoffset,
                 BlockContents* result);  //返回的heap_allocated, cachable都为true


// Implementation details follow.  Clients should ignore,

inline BlockHandle::BlockHandle()
    : offset_(~static_cast<uint64_t>(0)),
      size_(~static_cast<uint64_t>(0)) {
}

}  // namespace branchdb

#endif  // STORAGE_BRANCHDB_TABLE_FORMAT_H_
