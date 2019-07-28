// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "branchdb/options.h"

#include "branchdb/comparator.h"
#include "branchdb/env.h"
#include "branchdb/filter_policy.h"
#include "db/dbformat.h"


namespace branchdb {

Options::Options()
    : comparator(BytewiseComparator()), //若多个线程生成BytewiseComparator构造函数返回的是同一个对象的指针(但是因为其本身为线程安全的，使用其不需要额外同步),默认类型为BytewiseComparatorImpl, 但是数据库中即DBimpl中会重新赋值使用的InternalKeyComparator(comparator),并利用该重新复制的创建Mtable等对象
      create_if_missing(false),
      error_if_exists(false),
      paranoid_checks(false),
      env(Env::Default()), //若多个线程调用Env::Default()返回的是同一个对象的指针
      info_log(NULL),
//      write_buffer_size(4<<20),
//      write_buffer_size(32<<20), //即4MB->32MB
      write_buffer_size(128<<20), //即4MB->128MB
      max_open_files(1000),
      block_cache(NULL),
      block_size(4096),      //即4KB
      block_restart_interval(16),
      compression(kSnappyCompression),
      filter_policy(NULL),  //打开布隆过滤器(默认关闭)
      allocateMode(mNoAllocate), //默认不预留空间
      numMaxRangesForRO(0),      //在0-backThreadNum之间,默认关闭读优化
      numMaxThreadsDoingGC(0),   //在0-backThreadNum之间,默认关闭垃圾回收
      integratedAppendMerge(true),
      fixLevelandSeqs(false),
      maxAppendLevelIdx(1),  //设置为false时，下面两个变量无效，但是为了防止用户忘了赋值，这里设置一下
      maxSequences(2) { 
      
}

}  // namespace branchdb
