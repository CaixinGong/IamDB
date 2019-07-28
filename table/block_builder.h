// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_BRANCHDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_BRANCHDB_TABLE_BLOCK_BUILDER_H_

#include <vector>

#include <stdint.h>
#include "branchdb/slice.h"

namespace branchdb {

struct Options;

class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options);

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been callled since the last call to Reset(). 即 [Finish + Reset] + add
  // REQUIRES: key is larger than any previously added key(注意这个条件)
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t CurrentSizeEstimate() const; //返回的是将该data block写入文件的精确大小

  // Return true iff no entries have been added since the last Reset()
  bool empty() const {
    return buffer_.empty();
  }

 private:
  const Options*        options_;     // 使用了其block_restart_interval 和comparator
  std::string           buffer_;      // Destination buffer 保存着当前block，block的尾端在调用finish后加入
  std::vector<uint32_t> restarts_;    // Restart points 保存着每个restart点在block中的偏移量
  int                   counter_;     // Number of entries emitted since restart
  bool                  finished_;    // Has Finish() been called?
  std::string           last_key_;    //上一条的完整的key

  // No copying allowed
  BlockBuilder(const BlockBuilder&);
  void operator=(const BlockBuilder&);
};

}  // namespace branchdb

#endif  // STORAGE_BRANCHDB_TABLE_BLOCK_BUILDER_H_
