#include <utility>
#include <pthread.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>
#include <vector>

class Mutex {
 public:
  Mutex();
  ~Mutex();

  void Lock();
  bool Trylock();
  void Unlock();
  void AssertHeld() { }

 private:
  friend class CondVar;
  pthread_mutex_t mu_; //互斥量

  // No copying
  Mutex(const Mutex&);
  void operator=(const Mutex&);
};

static void PthreadCall(const char* label, int result) {
  if (result != 0) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
}
Mutex::Mutex() { PthreadCall("init mutex", pthread_mutex_init(&mu_, NULL)); }
Mutex::~Mutex() { PthreadCall("destroy mutex", pthread_mutex_destroy(&mu_)); }
void Mutex::Lock() { PthreadCall("lock", pthread_mutex_lock(&mu_)); }
bool Mutex::Trylock() {
 int re = pthread_mutex_trylock(&mu_);
 if(!(re==0 || re==EBUSY)) {
   assert(0);
   return false;
 }
 assert(re==0 || re==EBUSY);
 return (re == 0);
} 
void Mutex::Unlock() { PthreadCall("unlock", pthread_mutex_unlock(&mu_)); }

//需要多线程安全,因为在DoMainDiskJob函数中持有相应的R锁就可以调用本类的对象, 因为并行度要求不高故没有用读写锁
typedef std::pair<int64_t, int64_t> MyPair;
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

  bool setCapacity(int capacity) { //capacity只能赋值一次非零值
    assert(capacity > 0);
    if(capacity_ > 0)
      return false;
    capacity_ = capacity;
    storage_ = new MyPair[capacity_];
    assert(storage_ != NULL);
  }

  void setMaxPairNum(int maxPairNum) {
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
  }         //设置"存在的元素个数"

  int getMaxPairNum() {
    mutex_.Lock();
    assert(storage_ != NULL);
    int ret = maxPairNum_;
    mutex_.Unlock();
    return ret;
  }

  int getPairNum() {
    mutex_.Lock();
    assert(storage_ != NULL);
    int ret = pairNum_;
    mutex_.Unlock();
    return ret;
  }

  void insertPair(const MyPair& mypair) {   //插入元素
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

  enum sumTypes {
    byIterAll = 0,
    byModify
  };

  MyPair setSum(sumTypes sumtype = byModify) {
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

  bool isElemEnough() {      //当前存储是否达到了maxPairNum_个元素
    mutex_.Lock();
    assert(storage_ != NULL);
    assert(pairNum_ <= maxPairNum_);
    return pairNum_ == maxPairNum_;
    mutex_.Unlock();
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
  Mutex mutex_; 
};


int main() {
  const int cap = 100;
  PairFIFOPool pool(cap);

  //初始测试
  MyPair sum = pool.setSum();
  printf("%ld:%ld\n", sum.first, sum.second);  

  //插入不足最大
  pool.setMaxPairNum(10);
  for(int i = 0; i < 5; ++i) {
    MyPair pair(i, i+10);
    pool.insertPair(pair);
  }
  sum = pool.setSum();
  printf("%ld:%ld\n", sum.first, sum.second);  

  //插入达到最大
  for(int i = 0; i < 5; ++i) {
    MyPair pair(i, i+10);
    pool.insertPair(pair);
  }
  sum = pool.setSum();
  printf("%ld:%ld\n", sum.first, sum.second);  

  //插入超过最大
  for(int i = 0; i < 10; ++i) {
    MyPair pair(i, i+10);
    pool.insertPair(pair);
  }
  sum = pool.setSum();
  printf("%ld:%ld\n", sum.first, sum.second);  

  //扩大最大
  pool.setMaxPairNum(20);

  //插入达到新的最大
  for(int i = 0; i < 10; ++i) {
    MyPair pair(i, i+10);
    pool.insertPair(pair);
  }
  sum = pool.setSum();
  printf("%ld:%ld\n", sum.first, sum.second);  

  //扩大最大
  pool.setMaxPairNum(100);

  //插入达到新的最大
  for(int i = 100; i < 100+cap; ++i) {
    MyPair pair(i, i+10);
    pool.insertPair(pair);
  }
  sum = pool.setSum();
  printf("%ld:%ld\n", sum.first, sum.second);  

  //缩小最大
  pool.setMaxPairNum(50);
  for(int i = 100; i < 100+cap; ++i) {
    MyPair pair(i, i+10);
    pool.insertPair(pair);
  }
  sum = pool.setSum();
  printf("%ld:%ld\n", sum.first, sum.second);

  //缩小为0
  pool.setMaxPairNum(0);
  sum = pool.setSum();
  printf("%ld:%ld\n", sum.first, sum.second);  

  PairFIFOPool tmp;
  tmp.setCapacity(100);  
  tmp.setMaxPairNum(50);
  for(int i = 100; i < 100+cap; ++i) {
    MyPair pair(i, i+10);
    tmp.insertPair(pair);
  }
  sum = tmp.setSum();
  printf("%ld:%ld\n", sum.first, sum.second);

  return 0;
}

