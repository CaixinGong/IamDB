// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread safety
// -------------
//
// Writes require external synchronization, most likely(很可能) a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially(简单地，with litttle effort) guaranteed by the code since we
// never delete any skip list nodes.
//这里的析构函数使用合成的，因为没有动态分配的，动态分配的都在arena_中，析构arena_时会释放所有new的空间

//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <assert.h>
#include <stdlib.h>
#include "port/port.h"
#include "util/arena.h"
#include "util/random.h"

namespace branchdb {

class Arena;

template<typename Key, class Comparator>//注意这里的Comparator,不是一个接口，使用时用<const char*, KeyComparator>实例化模板
class SkipList {
 private:
  struct Node;//声明了嵌套类，一个私有的"类成员",当使用该类的完整定义时，编译器才定义该模板类

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit SkipList(Comparator cmp, Arena* arena);

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.(不允许插入相同的key)
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

  // Iteration over the contents of a skip list
  //Iterator 线程不安全，共享node_
  class Iterator {//一个公有类成员，供用户使用，注意与branchdb::Iterator抽象基类没有任何关系
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();//需从头节点开始遍历，若有倒叙遍历的需求（这里没有）可以使level 0层为双向链表

    // Advance to the first entry with a key >= target(不只是等于)
    void Seek(const Key& target);//与当前的结点无关，从头开始遍历

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private: //嵌套类的成员
    const SkipList* list_; //绑定的跳表
    Node* node_;//保存list_中当前节点的指针
    // Intentionally copyable
  }; //迭代器定义结束

 private:
  enum { kMaxHeight = 12 };/**************************************这个值是怎么定的？对于多大的数据量有最好的效率呢？*/

  // Immutable after construction
  Comparator const compare_; //注意这里的Comparator模板的一个类型参数，实例化后的类型应满足线程安全
                            // memtable中用 KeyComparator 实例化，比较时比较InternalKey, 调用其Compare方法
  Arena* const arena_;    // arena_为const线程安全，指向的对象线程不安全，故对于需要访问arena_的操作(写与写)如skiplist的插入需要用户同步

  Node* const head_;      //head_为const线程安全，指向的对象也线程安全

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  port::AtomicPointer max_height_;   // Height of the entire list 初始化是为1，线程安全

  inline int GetMaxHeight() const {
    return static_cast<int>(
        reinterpret_cast<intptr_t>(max_height_.NoBarrier_Load()));
  }

  // Read/written only by Insert().
  Random rnd_;  //线程不安全，但是只有插入的时候才访问

  Node* NewNode(const Key& key, int height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return NULL if there is no such node.
  //
  // If prev is non-NULL, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  // No copying allowed
  SkipList(const SkipList&);
  void operator=(const SkipList&);
};

// Implementation details follow
template<typename Key, class Comparator>  //Comparator 为comparator.h 中定义的接口
struct SkipList<Key,Comparator>::Node {
  explicit Node(const Key& k) : key(k) { }

  Key const key;	//两个成员:key,  next_[1]，使用时Key的类型为const char*，此时用户需要使用arena_额外分配键所需要的空间

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  Node* Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return reinterpret_cast<Node*>(next_[n].Acquire_Load());
  }
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_[n].Release_Store(x);//注意这是"."操作,注意这里n非零时，只分配了对象空间，未使用构造函数初始化
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return reinterpret_cast<Node*>(next_[n].NoBarrier_Load());
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].NoBarrier_Store(x);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  port::AtomicPointer next_[1];
}; //Node 定义结束

template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node*
SkipList<Key,Comparator>::NewNode(const Key& key, int height) {
  char* mem = arena_->AllocateAligned(
      sizeof(Node) + sizeof(port::AtomicPointer) * (height - 1));//Node中已经包含了1个AtomicPointer
  return new (mem) Node(key);//使用了定位new表达式：在mem中创建key对象
}

//迭代器的构造函数
template<typename Key, class Comparator>
inline SkipList<Key,Comparator>::Iterator::Iterator(const SkipList* list) {
  list_ = list;
  node_ = NULL;
}

template<typename Key, class Comparator>
inline bool SkipList<Key,Comparator>::Iterator::Valid() const {
  return node_ != NULL;
}

//从头开始遍历
template<typename Key, class Comparator>
inline const Key& SkipList<Key,Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);//返回小于key的最大的那个node的指针（可以为head_)
  if (node_ == list_->head_) {
    node_ = NULL;
  }
}

//返回的节点是大于等于的，不只是等于
template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, NULL);
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);//若为空list_，返回NULL
}

template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = NULL;
  }
}

template<typename Key, class Comparator>
int SkipList<Key,Comparator>::RandomHeight() {//返回值>=1，<=kMaxHeight即12
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;//以1/4的概率增长1, 那么最大为12层的平均高度为3层
  int height = 1;
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

//由此可知这里的实现为从小到大的增序排列: key >= n->key 返回真, 若n为NULL返回false
template<typename Key, class Comparator>
bool SkipList<Key,Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // NULL n is considered infinite
  return (n != NULL) && (compare_(n->key, key) < 0);
}

//作用1，向prev数组的max_height_个元素赋值为应插入的位置的第level层的前一个node的指针（从小到大排列），prev若为NULL则关闭该作用
//作用2，返回值为第一个>=等于当前key的node的指针
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* SkipList<Key,Comparator>::FindGreaterOrEqual(const Key& key, Node** prev)
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (KeyIsAfterNode(key, next)) { //key >= next-key返回真，若n为NULL返回false; 此处循环直到找到key在第level层应该插入
									 //的位置的前一个node赋给x，而next为在level层第一个大于等于该key的node ,再执行else
      // Keep searching in this list
      x = next;
    } else {
      if (prev != NULL) prev[level] = x;//prev数组保存要插入的结点的上一个结点
      if (level == 0) {
        return next;  //返回大于等于当前key的node*
      } else {
        // Switch to next list
        level--;
      }
    }
  }
}

//需从头开始遍历，若有倒叙遍历的需求(这里没有）可以使level 0为双向链表
//返回小于key的最大的那个node的指针（可以为head_)
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node*
SkipList<Key,Comparator>::FindLessThan(const Key& key) const {//返回小于key的最大的那个node的指针（可以为head_)
  Node* x = head_;                                            //需从头节点开始遍历
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);//当前要小于key(可以为*head_)
    Node* next = x->Next(level);
    if (next == NULL || compare_(next->key, key) >= 0) {//下一个大于等于key
      if (level == 0) {//在第0层,当上面三个条件都满足时返回
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {//下一个小于
      x = next;
    }
  }
}

//返回该跳表的最后一个节点，若跳表为空，则返回头节点（从最高层开始遍历，直至到第0层的下一个节点为NULL）
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* SkipList<Key,Comparator>::FindLast()
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == NULL) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template<typename Key, class Comparator> //注意这里的Comparator需要重载(key, key)操作符
SkipList<Key,Comparator>::SkipList(Comparator cmp, Arena* arena)//注意这里的Comparator是模板的类型
    : compare_(cmp), 
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)),   //NewNode使用了arena_->AllocateAligned分配空间
      max_height_(reinterpret_cast<void*>(1)),
      rnd_(0xdeadbeef) {
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, NULL);
  }
}

//插入要额外注意插入节点的设置顺序，先设置要插入节点的指向，后设置prev[i]指向的节点，且从level0开始，这样做是因为读可以不持有mutex_，见memtable的Get函数，或本contain函数
template<typename Key, class Comparator>
void SkipList<Key,Comparator>::Insert(const Key& key) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  Node* prev[kMaxHeight];//prev数组保存要插入的结点的上一个结点
  Node* x = FindGreaterOrEqual(key, prev);//作用1：向prev数组的max_height_个元素赋值为应插入的位置的第i层的前一个node的指针（从小到大排列）
                                          //作用2：返回第一个大于等于当前key的node* 
  // Our data structure does not allow duplicate insertion
  assert(x == NULL || !Equal(key, x->key));

  int height = RandomHeight();//返回值>=1，<=kMaxHeight即12
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {//对于新添加的层数，并且对于新添加的结点来说，他前向结点为*head_
      prev[i] = head_;
    }
    //fprintf(stderr, "Change height from %d to %d\n", max_height_, height);

    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (NULL), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since NULL sorts after all
    // keys.  In the latter case the reader will use the new node.
    // 上面也就是说，不管max_height_被读到新值还是旧值，都会正确的找到想要的结点
    // 只要node和指向该node的指针被正确的设置（设置了memory barrier）
    max_height_.NoBarrier_Store(reinterpret_cast<void*>(height));
  }

  x = NewNode(key, height);//新建一个结点
  for (int i = 0; i < height; i++) { //!注意这里的设置顺序，先设置x节点的指向，后设置prev[i]指向的节点，且从level0开始，这样做是因为读可以不持有mutex_
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].即下面第二句用了 barrier.
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));//改变新建节点的指针的指向,对于新添加的层，为NULL==head_指向的对应层
    prev[i]->SetNext(i, x);//改变新建节点的上一个结点的指向
  }
}

//若该skiplist包含key则返回ture,线程安全的，不需要持有mutex
template<typename Key, class Comparator>
bool SkipList<Key,Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, NULL); //返回大于等于当前key的node* 
  if (x != NULL && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

}  // namespace branchdb
