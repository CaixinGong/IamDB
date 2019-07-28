// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/memtable.h"
#include "db/dbformat.h"
#include "branchdb/comparator.h"
#include "branchdb/env.h"
#include "branchdb/iterator.h"
#include "util/coding.h"
namespace branchdb {

//返回实际value值（去除前面的length前缀）
static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& cmp)
    : comparator_(cmp),//MemTabel中嵌套类的对象,用了里面的构造函数
      refs_(0),
      table_(comparator_, &arena_) {//其中的arena_在refs_之后table_之前已经隐式的调用了默认构造函数初始化
}                                   //MemTable,Skiplist 用的都是同一分配内存空间的对象arena_

MemTable::~MemTable() {
  assert(refs_ == 0);
}

//只是近似值，具体看MemoryUsage的定义，显然这里需要额外的同步：多线程不安全
size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr)
    const {
  // Internal keys are encoded as length-prefixed strings.//即key_size + key
  Slice a = GetLengthPrefixedSlice(aptr);//返回实际value值（去除前面的length前缀）
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);//InternalKeyComparator 类型
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space. 
//将target做为data，编码 size(varint32) +  data ,写入scratch, 并返回scratch的data
static const char* EncodeKey(std::string* scratch, const Slice& target) {//Key 为const char*
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

//将SkipList中的公有迭代器类（线程不安全）做了简单的封装而已
class MemTableIterator: public Iterator { // Iterator 为Iterator.h定义的抽象基类
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) { }

  virtual bool Valid() const { return iter_.Valid(); }
  virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); }   
  virtual void SeekToFirst() { iter_.SeekToFirst(); }
  virtual void SeekToLast() { iter_.SeekToLast(); }
  virtual void Next() { iter_.Next(); }
  virtual void Prev() { iter_.Prev(); }
  virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); } //返回的是internal key
  virtual Slice value() const {  //返回当前节点的value值
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size()); 
  }

  virtual Status status() const { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_; //SkipList 中的公有类成员, 线程不安全
  std::string tmp_;       // For passing to EncodeKey, 线程不安全

  // No copying allowed
  MemTableIterator(const MemTableIterator&);
  void operator=(const MemTableIterator&);
};

Iterator* MemTable::NewIterator() {
  return new MemTableIterator(&table_);
}

//线程不安全，用了arena_共享变量分配skiplist的整个空间，table_的insert方法
//插入的格式为 internal key size(varint32)  + internal key(user key + tag(sequence number高7Bytes + type 1 Byte) ) 
//       + value size(varint32) + value  
void MemTable::Add(SequenceNumber s, ValueType type, // typedef uint64_t SequenceNumber; ValueType 是nameless enum 保存add的类型：插入还是删除
                   const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation(串联，连接) of:
  //  key_size     : varint32 of internal_key.size()//注意这里的key包含额外的8字节，共同组成internal_key，skiplist的比较基于internal_key，
  //  key bytes    : char[internal_key.size()]      //这样做的原因是skiplist是不允许重复的，加了额外的8字节就不会有重复 
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();//size_t为对象的长度，不带符号的
  size_t val_size = value.size();//当为删除时，val_size可以为0
  size_t internal_key_size = key_size + 8;//8字节：4字节为SequenceNumber s, 4字节为ValueType(enum)
  const size_t encoded_len =
      VarintLength(internal_key_size) + internal_key_size +
      VarintLength(val_size) + val_size; 
  char* buf = arena_.Allocate(encoded_len);
  char* p = EncodeVarint32(buf, internal_key_size); //返回的是编码后占用空间的下一个字节,这种编码方案对中小数字可节省空间
  memcpy(p, key.data(), key_size);  //internal key开始
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;						    //internal key 结束
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  assert((p + val_size) - buf == encoded_len);
  table_.Insert(buf);//内部并没有再分配空间存储buf的值,只是skiptlist的新建node的指针指向buf(Key[const char *]类型的一个直接赋值)
}                   //内部的插入Skiplist通过对buf中key_size:varint32的编解码获取key，并进行比较, 具体为使用了KeyComparator的()的操作符重载

//线程安全的函数，无需持有mutex
bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key(); //length + internal key(user key + tag)
  Table::Iterator iter(&table_);  //Skiplist 的公有类成员typedef SkipList<const char*, KeyComparator> Table; 注意iter线程不安全但是为局部变量
  iter.Seek(memkey.data()); //找出第一个>=的node
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();//这个key包含klength + internal key(user key + tag)+vlenght+value。没有iter.value()调用
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);//返回的指针为internal key的首地址, internal key 的长度在key_length中
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), 
            key.user_key()) == 0) { //只比较user key, user key 相等
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);//提取tag
      switch (static_cast<ValueType>(tag & 0xff)) {//提取add的类型
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);//返回的是value值
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

}  // namespace branchdb
