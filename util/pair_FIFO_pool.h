#ifndef STORAGE_BRANCHDB_PAIRFIFOPOOL_H_
#define STORAGE_BRANCHDB_PAIRFIFOPOOL_H_

#include <utility>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#include <vector>
#include "port/port.h"

namespace branchdb {

typedef std::pair<int64_t, int64_t> MyPair;

//需要多线程安全,因为在DoMainDiskJob函数中持有相应的R锁就可以调用本类的对象, 因为并行度要求不高故没有用读写锁

class PairFIFOPool { //类似双端队列实现FIFO逻辑,但是不同的是，有capacity和maxPairNum两个逻辑,其中maxPairNum <= capacity
 public:
  PairFIFOPool(int capacity = 0): capacity_(capacity), maxPairNum_(0), pairNum_(0), startIdx_(0), endIdx_(0), sum_(0,0), storage_(NULL) {
    if(capacity_ > 0) {
      storage_ = new MyPair[capacity_];
      assert(storage_ != NULL);
    }
  }
  ~PairFIFOPool() {
    mutex_.Lock();
    if(storage_)
      delete [] storage_;
    mutex_.Unlock();
  }
  bool setCapacity(int capacity) { //capacity只能赋值一次非零值(可能包括构造函数一次), 否则返回false
    assert(capacity > 0);
    if(capacity_ > 0)
      return false;
    capacity_ = capacity;
    storage_ = new MyPair[capacity_];
    assert(storage_ != NULL);
    return true;
  }

  void setMaxPairNum(int maxPairNum); //设置此FIFO pool允许拥有的最多元素个数

  int getMaxPairNum() {
    mutex_.Lock();
    assert(storage_ != NULL);
    int ret = maxPairNum_;
    mutex_.Unlock();
    return ret;
  }

  int getPairNum() { //返回存储的元素个数
    mutex_.Lock();
    assert(storage_ != NULL);
    int ret = pairNum_;
    mutex_.Unlock();
    return ret;
  }

  void insertPair(const MyPair& mypair);   //插入元素

  enum sumTypes {
    byIterAll = 0, //通过遍历所有进行sum
    byModify       //通过增量获取所有
  };

  MyPair setSum(sumTypes sumtype = byModify); //返回得到的sum

  bool isElemEnough() {      //当前存储是否达到了maxPairNum_个元素
    mutex_.Lock();
    assert(storage_ != NULL);
    assert(pairNum_ <= maxPairNum_);
    int pN = pairNum_;
    int mPN = maxPairNum_;
    mutex_.Unlock();
    return pN == mPN;
  }

 private:
  int capacity_;  //capacity只能赋值一次非零值,因为应用场景不需要赋值多次
  int maxPairNum_;
  int pairNum_;
  int startIdx_; //当startIdx_和endIdx_相等时,表示未存储元素 或 存储元素满了且元素的个数为capacity (结合pairNum_可以判断哪种状态)
  int endIdx_;  //若不空，指向尾元素的下一个位置即将插入的元素的位置
  MyPair sum_;
  MyPair addedPairSum_; //这两个变量为sum_被赋值至今storage_中变化之和
  MyPair removedPairSum_;

  MyPair* storage_;
  port::Mutex mutex_;
};


}

#endif
