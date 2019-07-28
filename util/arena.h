// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_BRANCHDB_UTIL_ARENA_H_
#define STORAGE_BRANCHDB_UTIL_ARENA_H_

#include <vector>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>

namespace branchdb {

class Arena {
 public:
  Arena();
  ~Arena();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  char* Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc
  char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena (including space allocated but not yet used for user
  // allocations).
  size_t MemoryUsage() const {
    return blocks_memory_ + blocks_.capacity() * sizeof(char*);//前面一部分是动态分配的空间，后面一部分是vector占用的空间
  }

 private:
  char* AllocateFallback(size_t bytes);
  char* AllocateNewBlock(size_t block_bytes);

  // Allocation state
  char* alloc_ptr_;//当前块中，空闲的区域的起始地址的指针
  size_t alloc_bytes_remaining_;//当前块空闲空间的字节数

  // Array of new[] allocated memory blocks
  std::vector<char*> blocks_;//block的个数vector本身有

  // Bytes of memory in blocks allocated so far（是所有blocks的总大小）
  size_t blocks_memory_;//单位字节

  // No copying allowed
  Arena(const Arena&);
  void operator=(const Arena&);
};

inline char* Arena::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
  assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) {//没有分配空间（如第一次执行）这里不会执行;
                                        //且这里会有空间浪费,若bytes<1KB,且剩余的空间不足，则这些空间不再利用.
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  return AllocateFallback(bytes);
}

}  // namespace branchdb

#endif  // STORAGE_BRANCHDB_UTIL_ARENA_H_
