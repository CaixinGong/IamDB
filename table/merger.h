// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_BRANCHDB_TABLE_MERGER_H_
#define STORAGE_BRANCHDB_TABLE_MERGER_H_

namespace branchdb {

class Comparator;
class Iterator;

// Return an iterator that provided the union of the data in
// children[0,n-1].  Takes ownership of the child iterators and
// will delete them when the result iterator is deleted.
//
// The result does no duplicate suppression.  I.e., if a particular
// key is present in K child iterators, it will be yielded K times.
//
// REQUIRES: n >= 0
//设计思想：由多个迭代器组成，每个迭代器下的元素已经排号序
//MergingIterator的所有函数与多个迭代器下的元素merge后生成的新的迭代器的效果一样
extern Iterator* NewMergingIterator(
    const Comparator* comparator, Iterator** children, int n);

}  // namespace branchdb

#endif  // STORAGE_BRANCHDB_TABLE_MERGER_H_
