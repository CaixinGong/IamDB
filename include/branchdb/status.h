// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

#ifndef STORAGE_BRANCHDB_INCLUDE_STATUS_H_
#define STORAGE_BRANCHDB_INCLUDE_STATUS_H_

#include <string>
#include "branchdb/slice.h"

namespace branchdb {

class Status {//只有一个成员变量 const char* state_,记录相应状态信息
 public:
  // Create a success status.
  Status() : state_(NULL) { }
  ~Status() { delete[] state_; }

  // Copy the specified status.
  Status(const Status& s);
  void operator=(const Status& s);

  // Return a success status.
  static Status OK() { return Status(); }

  // Return error status of an appropriate type.
  static Status NotFound(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kNotFound, msg, msg2);
  }
  static Status Corruption(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kCorruption, msg, msg2);
  }
  static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kNotSupported, msg, msg2);
  }
  static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kInvalidArgument, msg, msg2);
  }
  static Status IOError(const Slice& msg, const Slice& msg2 = Slice()) {//包含char* 或string->Slice的隐式类型转换
    return Status(kIOError, msg, msg2);//kIOError为enum，==5,设置Status类的成员变量const char* state_
  }
  static Status FileHoleOnlyHoldIndex(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kFileHoleOnlyHoldIndex, msg, msg2);
  }
  static Status FileHoleSmall(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kFileHoleSMall, msg, msg2);
  }

  // Returns true iff the status indicates success.
  bool ok() const { return (state_ == NULL); }

  // Returns true iff the status indicates a NotFound error.
  bool IsNotFound() const { return code() == kNotFound; }

  // Returns true iff the status indicates a Corruption error.
  bool IsCorruption() const { return code() == kCorruption; }

  // Returns true iff the status indicates an IOError.
  bool IsIOError() const { return code() == kIOError; }

  // Returns true iff the file hole is small only hold index.
  bool IsFileHoleOnlyHoldIndex() const { return code() == kFileHoleOnlyHoldIndex; }

  // Returns true iff the file hole is small can't even hold index.
  bool IsFileHoleSmall() const { return code() == kFileHoleSMall; }

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  //将code转换为的可读字符串 + ": " + msg部分 连接起来构造一个string返回
  std::string ToString() const;  //定义不再此文件中, 见util/status.cc

 private:
  // OK status has a NULL state_.  Otherwise, state_ is a new[] array
  // of the following form:
  //    state_[0..3] == length of message //即下面message的长度
  //    state_[4]    == code  //即enum Code类型
  //    state_[5..]  == message  //为msg1(: msg2),若msg2不存在为len1，若msg2存在为len1+2+len2字节
  const char* state_;

  enum Code {
    kOk = 0,
    kNotFound = 1,
    kCorruption = 2,   //坏块, 如读block时小于要读的长度; 数据与预期的不一样等
    kNotSupported = 3,
    kInvalidArgument = 4,
    kIOError = 5,      //读写磁盘时发生错误,如一个字节也没有读到;打开文件失败等
    kFileHoleOnlyHoldIndex = 6,
    kFileHoleSMall = 7
  };

  Code code() const {
    return (state_ == NULL) ? kOk : static_cast<Code>(state_[4]);
  }

  Status(Code code, const Slice& msg, const Slice& msg2);//定义不再此文件中, 见util/status.cc
  //将s保存的所有内容完整的拷贝到new的内存中，并返回
  static const char* CopyState(const char* s);		//定义不再此文件中, 见util/status.cc
};

inline Status::Status(const Status& s) {
  state_ = (s.state_ == NULL) ? NULL : CopyState(s.state_);
}
inline void Status::operator=(const Status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are ok.
  if (state_ != s.state_) {
    delete[] state_;
    state_ = (s.state_ == NULL) ? NULL : CopyState(s.state_);
  }
}

}  // namespace branchdb

#endif  // STORAGE_BRANCHDB_INCLUDE_STATUS_H_
