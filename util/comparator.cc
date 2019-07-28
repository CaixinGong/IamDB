// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <stdint.h>
#include "branchdb/comparator.h"
#include "branchdb/slice.h"
#include "port/port.h"
#include "util/logging.h"

namespace branchdb {

Comparator::~Comparator() { }

namespace {// 匿名的命名空间，其定义局部于特定文件，从不跨越多个文件
//该类进行常规的字符序比较
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() { }

  virtual const char* Name() const {
    return "branchdb.BytewiseComparator";
  }

  virtual int Compare(const Slice& a, const Slice& b) const {
    return a.compare(b);
  }

  // If *start < limit, changes *start to a short string in [start,limit).
  //return with *start unchanged 是正确的,这里的实现不是最好的，如：
  //abcdefg 和ac,此函数将不改变*start返回abcdfeg，显然ab更好
  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {//输入的和返回的*start必须<limit
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
        assert(Compare(*start, limit) < 0);
      }
    }
  }

  //找最短的successor(后继者), key既为输入也为输出
  virtual void FindShortSuccessor(std::string* key) const {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i+1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }

  //求a,b之间中间的string, 默认向下取整;(注意是左对齐的比较的,最右边为最低位), plusOne为true后，会对求平均值后的结果+1返回.
  virtual std::string MiddleString(const Slice& a, const Slice& b, bool plusOne=false) const {
    int minLen = a.size() > b.size() ? b.size() : a.size();
    int maxLen = a.size() > b.size() ? a.size() : b.size();
    short* sum =  new short[maxLen + 1];
    memset(sum, 0, (maxLen+1)*sizeof(short));
    short* mid =  new short[maxLen + 1];

    //求和(注意是左对齐的比较的,所以是从最右位开始加,最右边为最低位)
    short carryBit = 0;
    for (int i = minLen-1; i >= 0; --i) {
      sum[i+1] = (unsigned char)a[i] + (unsigned char)b[i] + carryBit;
      carryBit = sum[i+1] >= 256 ? 1 : 0;
      sum[i+1] = sum[i+1] % 256;
    }
    sum[0] = carryBit;
    const Slice* longerOne = a.size() > b.size() ? &a: &b;
    for (int i = minLen; i < maxLen; ++i) {
      sum[i+1] = (unsigned char)(*longerOne)[i];
    }
    //求和除以2的结果mid
    for (int i = 0; i < maxLen + 1; ++i) {
      mid[i] =  sum[i] / 2;
      if (i + 1 < maxLen + 1)
        sum[i+1] += ((sum[i] % 2) * 256);
    }
    assert(mid[0] == 0);

    if(plusOne) { // 对得到的求和结果+1
      assert(Compare(a,b) != 0);
      carryBit = 1;
      for (int i = maxLen; i >= 0; --i) {
        mid[i] =  mid[i] + carryBit;
        carryBit = mid[i] >= 256 ? 1 : 0;
        mid[i] = mid[i] % 256;
      }
      assert(mid[0] == 0);
    }

    //将short数组转换为string
    std::string result(maxLen, '0');
    for(int i = 0 ; i < maxLen; ++i) {
      assert(mid[i+1] >= 0 && mid[i+1] <= 255);
      result[i] = mid[i+1];
    }
    assert(result.size() ==  (a.size() > b.size() ? a.size() : b.size()) );

  #ifndef NDEBUG
    assert(result.size() ==  maxLen );
    Slice smallerOne = Compare(a, b) < 0 ? a : b;
    Slice largerOne = Compare(a, b) < 0 ? b : a;
    if(plusOne) {
      assert(Compare(largerOne, smallerOne) != 0);
      assert(Compare(result, largerOne) <=0 && Compare(result, smallerOne)>0);
    } else {
      assert(Compare(result, largerOne) < 0 && Compare(result, smallerOne)>=0 || Compare(largerOne,smallerOne) == 0);
    }
  #endif

    delete [] sum;
    delete [] mid;
    return result;
  }
  
};

}  // namespace

static port::OnceType once = BRANCHDB_ONCE_INIT; //caixin:typedef pthread_once_t OnceType; #define BRANCHDB_ONCE_INIT PTHREAD_ONCE_INIT
static const Comparator* bytewise;

static void InitModule() {
  bytewise = new BytewiseComparatorImpl;
}

const Comparator* BytewiseComparator() {
  port::InitOnce(&once, InitModule); //多线程之间只会执行一次
  return bytewise; //所以若多个线程生成BytewiseComparator构造函数返回的是同一个对象的指针(但是因为其本身为线程安全的，使用其不需要额外同步)
}

}  // namespace branchdb
