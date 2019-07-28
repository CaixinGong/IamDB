#include "pair_FIFO_pool.h"

namespace branchdb {

void PairFIFOPool::setMaxPairNum(int maxPairNum) {
  mutex_.Lock();
  assert(storage_ != NULL);
  assert(maxPairNum >= 0 && maxPairNum <= capacity_ && pairNum_ <= maxPairNum_);
  if(maxPairNum < pairNum_) { //删除了元素
    int rmElemNum = pairNum_ - maxPairNum;
    assert(rmElemNum > 0);
    for(int i = 0; i < rmElemNum; ++i) {
      removedPairSum_.first += storage_[(startIdx_ + i)%capacity_].first;
      removedPairSum_.second += storage_[(startIdx_ + i)%capacity_].second;
    }
    startIdx_ = (startIdx_ + rmElemNum) % capacity_; //去掉先进来的元素
    pairNum_ -= rmElemNum;

  }
  maxPairNum_ = maxPairNum;
  mutex_.Unlock();
}

void PairFIFOPool::insertPair(const MyPair& mypair) {   //插入元素
  mutex_.Lock();
  assert(storage_ != NULL);
  assert(pairNum_ == ((endIdx_ - startIdx_ + capacity_) % capacity_) || pairNum_ == capacity_ );
  assert(pairNum_ <= maxPairNum_ && maxPairNum_ > 0);

  addedPairSum_.first += mypair.first;
  addedPairSum_.second += mypair.second;
  if(pairNum_ < maxPairNum_ ) { //不满
    storage_[endIdx_] = mypair;
    endIdx_ = (endIdx_+1)%capacity_;
    ++pairNum_;
  }
  else { //相等,即满了
    removedPairSum_.first += storage_[startIdx_].first;
    removedPairSum_.second += storage_[startIdx_].second;
    startIdx_ = (startIdx_ + 1) % capacity_; //去掉最先进来的元素
    storage_[endIdx_] = mypair;
    endIdx_ = (endIdx_+1)%capacity_;
  }
  mutex_.Unlock();
}

MyPair PairFIFOPool::setSum(sumTypes sumtype ) { //默认参数为byModify
  mutex_.Lock();
  assert(storage_ != NULL);
  MyPair sumTmp(0,0);
  if(sumtype == byIterAll) {
    sumTmp= std::make_pair(0,0);
    for(int i = 0; i < pairNum_; ++i) {
      sumTmp.first += storage_[(startIdx_+i)%capacity_].first;
      sumTmp.second += storage_[(startIdx_+i)%capacity_].second;
    }
  }
  else if (sumtype == byModify) {
    sumTmp = sum_;
    sumTmp.first = sumTmp.first + addedPairSum_.first - removedPairSum_.first;
    sumTmp.second = sumTmp.second + addedPairSum_.second - removedPairSum_.second;
  }
  else {
    assert(0);
  }
  addedPairSum_ = std::make_pair(0,0);
  removedPairSum_ = std::make_pair(0,0);
  sum_ = sumTmp;
  mutex_.Unlock();
  return sumTmp;
}

}

