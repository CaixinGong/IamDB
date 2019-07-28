// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/format.h"

#include "branchdb/env.h"
#include "port/port.h"
#include "table/block.h"
#include "util/coding.h"
#include "util/crc32c.h"

#include <iostream>

namespace branchdb {

//将offset_ , size_的值编码为varint64，并append到*dst
void BlockHandle::EncodeTo(std::string* dst) const {
  // Sanity check that all fields have been set
//  assert(offset_ != ~static_cast<uint64_t>(0));
//  assert(size_ != ~static_cast<uint64_t>(0));
  PutVarint64(dst, offset_);
  PutVarint64(dst, size_);
}

//将input中的varint64的两个值 解码到对象的offset_ , size_ ,并且input中的指针前移
Status BlockHandle::DecodeFrom(Slice* input) {
  if (GetVarint64(input, &offset_) &&
      GetVarint64(input, &size_)) {
    return Status::OK();
  } else {
    return Status::Corruption("bad block handle");
  }
}

//将.sst文件中的Footer内容全部写入dst
void Footer::EncodeTo(std::string* dst) const {
#ifndef NDEBUG
  const size_t original_size = dst->size();
#endif
  filter_handle_.EncodeTo(dst);//将offset_ , size_的值编码为varint64，并append到*dst
  index_handle_.EncodeTo(dst);
  dst->resize(2 * BlockHandle::kMaxEncodedLength);  // Padding ,这样有40bits
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));// 对dst进行append操作,append低32位（小端存储）
  PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));// 对dst进行append操作,append高32位（小端存储）共8bits
  assert(dst->size() == original_size + kEncodedLength);
}

//input: .sst文件中的Footer的所有内容,函数结束后,将input中的指针移向解析了Footer之后的位置
//写Footer对象中的metaindex_handle_ 和 index_handle_
Status Footer::DecodeFrom(Slice* input) {
  //获得magic number
  const char* magic_ptr = input->data() + kEncodedLength - 8;
  const uint32_t magic_lo = DecodeFixed32(magic_ptr);
  const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
  const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                          (static_cast<uint64_t>(magic_lo)));
  if (magic != kTableMagicNumber) {
    return Status::InvalidArgument("not an sstable (bad magic number)");
  }
  //填充metaindex_handle_ 和 index_handle_
  Status result = filter_handle_.DecodeFrom(input);//将input中的varint64的两个值 解码到metaindex_handle 的offset_ , size_,
                                                      //并且input中的指针前移
  if (result.ok()) {
    result = index_handle_.DecodeFrom(input); //input中的指针前移
  }
  //将input中的指针移向解析了Footer之后的位置
  if (result.ok()) {
    // We skip over any leftover data (just padding for now) in "input"
    const char* end = magic_ptr + 8;
    *input = Slice(end, input->data() + input->size() - end);
  }
  return result;
}

//读文件相应block(任意类型的block）的内容(不包括type+crc)到result中:result的内容(data成员)，以及 cachable, heap_allocated 两个bool变量被设置
//后两个bool变量根据file_的实际类型和该block是否压缩策略决定,若为true内存的释放由用户进行(即Block的析构函数进行)
Status ReadBlock(RandomAccessFile* file,
                 const ReadOptions& options,
                 const BlockHandle& handle,
                 BlockContents* result) {
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;//后面根据情况可能被设置为ture

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size());
  char* buf = new char[n + kBlockTrailerSize];//kBlockTrailerSize 为5（包括type（压缩策略1byte） + crc（4 bytes） ）
  Slice contents;
  Status s = file->Read(handle.offset(), n + kBlockTrailerSize, &contents, buf);//根据file的实际类型，从文件中的offset开始出读n个字节
                                                               //(可能到buf中)，contents的内容可能与buf关联,也可能file的实际类型内部分配空间
  if (!s.ok()) { //错误
    delete[] buf;
    return s;
  }
  if (contents.size() != n + kBlockTrailerSize) {//错误
    delete[] buf;
    return Status::Corruption("truncated block read");
  }

  // Check the crc of the type and the block contents
  const char* data = contents.data();    // Pointer to where Read put the data
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      delete[] buf;
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }
  //根据不同的压缩类型采取不同的策略:构造result并返回
  switch (data[n]) { //data[n]为压缩类型
    case kNoCompression:
      if (data != buf) {//判断空间的使用策略（有file的实际类型决定）// 若不相等，file自己管理，cacheable等标记设置为false 
        // File implementation gave us pointer to some other data.
        // Use it directly under the assumption that it will be live
        // while the file is open.
        delete[] buf;
        result->data = Slice(data, n);
        result->heap_allocated = false;
        result->cachable = false;  // Do not double-cache, 只要对应的文件没有关闭，内容是保存在内存中的
      } else {
        result->data = Slice(buf, n);
        result->heap_allocated = true;//这种情况内存的释放由用户进行(Block的析构函数进行)
        result->cachable = true;
      }

      // Ok
      break;
    case kSnappyCompression: {
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        delete[] buf;
        return Status::Corruption("corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        delete[] buf;
        delete[] ubuf;
        return Status::Corruption("corrupted compressed block contents");
      }
      delete[] buf;
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;//这种情况内存的释放由用户进行(Block的析构函数进行)
      result->cachable = true;
      break;
    }
    default:
      delete[] buf;
      return Status::Corruption("bad block type");
  }

  return Status::OK();
}

//注意返回result中的内存都是从动态分配的, 即result->heap_allocated, result->cachable都为true
Status ReadBlockFromMem(const char* allData,
                 const ReadOptions& options,
                 const BlockHandle& handle,
		 uint64_t baseoffset,
                 BlockContents* result) { //返回的heap_allocated, cachable都为true
  result->data = Slice();
  result->cachable = false;
  result->heap_allocated = false;//后面根据情况可能被设置为ture

  // Read the block contents as well as the type/crc footer.
  // See table_builder.cc for the code that built this structure.
  size_t n = static_cast<size_t>(handle.size());
  const char* data= allData + handle.offset() - baseoffset;//kBlockTrailerSize 为5（包括type（压缩策略1byte） + crc（4 bytes） ）

  Status s;
  // Check the crc of the type and the block contents
  if (options.verify_checksums) {
    const uint32_t crc = crc32c::Unmask(DecodeFixed32(data + n + 1));
    const uint32_t actual = crc32c::Value(data, n + 1);
    if (actual != crc) {
      s = Status::Corruption("block checksum mismatch");
      return s;
    }
  }
  //根据不同的压缩类型采取不同的策略:构造result并返回
  switch (data[n]) { //data[n]为压缩类型
    case kNoCompression: {
	char *buf = new char[n];
	memcpy(buf, data, n);
        result->data = Slice(buf, n);
        result->heap_allocated = true;//这种情况内存的释放由用户进行(Block的析构函数进行)
        result->cachable = true;
      }
      break;
    case kSnappyCompression: {
      size_t ulength = 0;
      if (!port::Snappy_GetUncompressedLength(data, n, &ulength)) {
        return Status::Corruption("corrupted compressed block contents");
      }
      char* ubuf = new char[ulength];
      if (!port::Snappy_Uncompress(data, n, ubuf)) {
        return Status::Corruption("corrupted compressed block contents");
      }
      result->data = Slice(ubuf, ulength);
      result->heap_allocated = true;//这种情况内存的释放由用户进行(Block的析构函数进行)
      result->cachable = true;
      break;
    }
    default:
      return Status::Corruption("bad block type");
  }

  return Status::OK();
}



}  // namespace branchdb
