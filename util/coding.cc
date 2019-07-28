// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/coding.h"

namespace branchdb {

//编码后，buf的内容是小端的
void EncodeFixed32(char* buf, uint32_t value) {
  if (port::kLittleEndian) { //小端的机器，直接拷贝
    memcpy(buf, &value, sizeof(value));
  } else {//大端机器
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
  }
}

//编码后，buf的内容是小端的
void EncodeFixed64(char* buf, uint64_t value) {
  if (port::kLittleEndian) {//小端的机器，直接拷贝
    memcpy(buf, &value, sizeof(value));
  } else {//大端的机器
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
    buf[4] = (value >> 32) & 0xff;
    buf[5] = (value >> 40) & 0xff;
    buf[6] = (value >> 48) & 0xff;
    buf[7] = (value >> 56) & 0xff;
  }
}
//下面两个函数是上面两个函数的high level, 都是append，而不是覆盖
void PutFixed32(std::string* dst, uint32_t value) {
  char buf[sizeof(value)];
  EncodeFixed32(buf, value);
  dst->append(buf, sizeof(buf));
}

void PutFixed64(std::string* dst, uint64_t value) {
  char buf[sizeof(value)];
  EncodeFixed64(buf, value);
  dst->append(buf, sizeof(buf));
}

//return a pointer just past the last byte written.编码后的值写入dst，这种编码方案对中小数字可节省空间
//对于大数字，可能占用5bytes, 故调用时，dst至少需5个字节，写入的长度为:返回值-dst
char* EncodeVarint32(char* dst, uint32_t v) {
  // Operate on characters as unsigneds
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  static const int B = 128;         //第7位为1(从0开始计数)
  if (v < (1<<7)) {        //小于第7位为1(1 bytes)
    *(ptr++) = v;               //第7位为0(因为小于第7位为1)
  } else if (v < (1<<14)) {//小于第14位为1(2 bytes)
    *(ptr++) = v | B;            //第7位设置为1,0-6位保持不变
    *(ptr++) = v>>7;             //第15位为0(因为小于第14位为0)
  } else if (v < (1<<21)) {//小于第21位为1(3 bytes)
    *(ptr++) = v | B;            //第7位设置为1,0-6位保持不变
    *(ptr++) = (v>>7) | B;       //第15为设置为1，7-14位保持不变 
    *(ptr++) = v>>14;            //第23为设置为0(因为小于第21为1）
  } else if (v < (1<<28)) {//小于第28位为1(4bytes)
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = v>>21;
  } else {               //其他32位无符号整数，这里有5 bytes
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = (v>>21) | B;
    *(ptr++) = v>>28;
  }
  return reinterpret_cast<char*>(ptr);
}

void PutVarint32(std::string* dst, uint32_t v) {
  char buf[5];
  char* ptr = EncodeVarint32(buf, v);
  dst->append(buf, ptr - buf);
}
//return a pointer just past the last byte written.编码后的值写入dst，这种编码方案对中小数字可节省空间
//这里调用该函数是，dst至少为10位, 因为7*9< 64 <7*10,写入的长度为:返回值-dst
char* EncodeVarint64(char* dst, uint64_t v) {
  static const int B = 128;
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  while (v >= B) {
    *(ptr++) = (v & (B-1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v);//对于最后一个byte（第7位为0)
  return reinterpret_cast<char*>(ptr);
}

void PutVarint64(std::string* dst, uint64_t v) {
  char buf[10];
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, ptr - buf);
}

//该函数将*dst(string) append value.size(类型varint32) 和 value本身
void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
  PutVarint32(dst, value.size());//dst append了value.size的varint32
  dst->append(value.data(), value.size());//dst append 了value本身
}

//返回v编码为varint 后的长度
int VarintLength(uint64_t v) {
  int len = 1;
  while (v >= 128) {
    v >>= 7;
    len++;
  }
  return len;
}


// These routines only look at bytes in the range [p..limit-1]小端存储
// 解码后的值写入value中，返回值为解码的bytes的后一个
const char* GetVarint32PtrFallback(const char* p,
                                   const char* limit,
                                   uint32_t* value) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {//p指向的为小端存储，故shift由小到大
    uint32_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);//第7位一定为1
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return NULL;//指定非返回内不包含一个有意义的编码
}

// Standard Get... routines parse a value from the beginning of a Slice
// and advance the slice past the parsed value.返回值写回value, input指针前移(新建了一个Slice对象)
bool GetVarint32(Slice* input, uint32_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint32Ptr(p, limit, value);//coding.h中定义
  if (q == NULL) {
    return false;
  } else {
    *input = Slice(q, limit - q);
    return true;
  }
}

// These routines only look at bytes in the range [p..limit-1]小端存储
// 解码后的值写入value中，返回值为解码的bytes的后一个
const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return NULL;//指定的范围内的开头无有意义编码
}

// Standard Get... routines parse a value from the beginning of a Slice
// and advance the slice past the parsed value.返回值写回value, input指针前移(新建了一个Slice对象)
bool GetVarint64(Slice* input, uint64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint64Ptr(p, limit, value);
  if (q == NULL) {
    return false;
  } else {
    *input = Slice(q, limit - q);
    return true;
  }
}
//见 PutLengthPrefixedSlice,该函数将*dst(string) append了 value.size的varint32 和 value本身
//result 为value的值， 返回值为解析的bytes的后一字节
const char* GetLengthPrefixedSlice(const char* p, const char* limit,
                                   Slice* result) {
  uint32_t len;
  p = GetVarint32Ptr(p, limit, &len);//提取出前面的size, 并前移p指针
  if (p == NULL) return NULL;
  if (p + len > limit) return NULL;
  *result = Slice(p, len);//提取出后面的内容
  return p + len;//前移p指针
}

//功能与前一个函数类似，只不过改变了输入的格式，和返回值的格式(在input中，通过前移解析了bytes的指针），result仍为value值
bool GetLengthPrefixedSlice(Slice* input, Slice* result) {
  uint32_t len;
  if (GetVarint32(input, &len) &&
      input->size() >= len) {
    *result = Slice(input->data(), len);
    input->remove_prefix(len);
    return true;
  } else {
    return false;
  }
}

}  // namespace branchdb
