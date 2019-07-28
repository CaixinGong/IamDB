// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "branchdb/mtable_builder.h"

#include <assert.h>
#include "branchdb/comparator.h"
#include "branchdb/env.h"
#include "branchdb/filter_policy.h"
#include "branchdb/options.h"
#include "branchdb/status.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "db/dbformat.h"

namespace branchdb {

//私有类成员
struct MtableBuilder::Rep {
  Options options;
  Options index_block_options;  //与options唯一不同的是index_block_options.block_restart_interval=1
  PwritableFile* file;
  uint64_t offset1;// 当前data block若写入sstable文件,在MSStable中的起始偏移，可用于设置pending_handle 
  uint64_t offset2;// 当前索引数据的起始偏移量(在Finish中会变化)
  const uint64_t base_offset1; //此次builder写的第一个data block起始的offset
  const uint64_t base_offset2; //若builder失败可用于恢复offset2, 同样上面可恢复offset1
  const uint64_t oldHoleSize;
  Status status;  //当前状态-初始ok
  BlockBuilder data_block;//当前操作的data block 
  BlockBuilder index_block;
  std::string last_key;//当前data block最后的k/v对的key
  int64_t num_entries;//本次已插入的所有记录的个数(即本次调用add的次数)
  bool closed;        // Either Finish() or Abandon() has been called.(只由这两个函数设置)
  bool isAbandoned;
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry (其中r为：Rep* r = rep_)is true only if data_block is empty.
  // 注意下面两个只有当.sst已经写入一个data block时(写时设置下面两个值，一个为true，一个为这个data block的BlockHandle)
  // 写下一个data block 的第一条记录时才将pending handle（上一个data block的）写入index block
  bool pending_index_entry;  //初始为false(表明第一个data block的index不需要显式的存储), 只有.sst刚写入一个data block,这个才被设置为true
  BlockHandle pending_handle;  // Handle to add to index block(index block的value值包括offset(varint64) 和size(varint64) )
  std::string pre_data;
  std::string pre_index;

  std::string compressed_output;//压缩后的data block，临时存储，写入后即被清空

  // 下面的status、last_key、pending_handle、compressed_output 调用了默认构造函数
  // 生成了一个空的data block, index block 并且绑定了一个文件(.sst),根据选项可能生成filter block
  Rep(const Options& opt, PwritableFile* f, uint64_t o1, uint64_t o2, uint64_t hole_Size)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset1(o1),
        offset2(o2),
        base_offset1(offset1),
        base_offset2(offset2),
        oldHoleSize(hole_Size),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        isAbandoned(false),
        filter_block(opt.filter_policy == NULL ? NULL
                     : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1; //设置index block 的restarts poinst[i] 为聚集索引
  }
};

// 生成了一个空的data block, index block 并且绑定了一个文件(.bdb),根据选项可能生成filter block, 为文件预分配磁盘空间等
// offset1=offset2=0表示要创建的为SStable类型的，只打算存储一个sorted string
MtableBuilder::MtableBuilder(const Options& options, PwritableFile* file, uint64_t offset1, uint64_t offset2, uint64_t holeSize, Status& r)\
  : rep_(new Rep(options, file, offset1, offset2, holeSize)) {
  if (rep_->filter_block != NULL) {
    rep_->filter_block->StartBlock(0);   //这里无需做改变, offset1 - base_offset1
//    rep_->filter_block->StartBlock(rep_->base_offset1);
  }
  if(options.allocateMode != mNoAllocate && rep_->offset1==0 && rep_->offset2== config::fileCommonSize) {//SStable格式的(offset1,offset2都为0)不预分配空间
    if(options.allocateMode == mEndAllocate) {
      int64_t pageSize = 1024 * 4; //4k
      int64_t pageNum = ((config::fileCommonSize>>6) + pageSize -1)/pageSize; //只预留1/64,4K对应<=64字节即可将所有索引数据存放在预留空间中(包括bloom filter)
      int64_t allocateSize = pageNum * pageSize;
      r = rep_->file->AllocateSpace(config::fileCommonSize-allocateSize, allocateSize);
    }
    else if(options.allocateMode == mAllAllocate) {
      r = rep_->file->AllocateSpace(0, config::fileCommonSize);
    }
    else 
      assert(0);
  }
}

MtableBuilder::~MtableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block; //这句写入Rep类的析构函数比较好
  delete rep_;
}

Status MtableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) { //Comparator的实现接口是不能换的
    return Status::InvalidArgument("changing comparator while building table");
  }
  if (options.filter_policy != rep_->options.filter_policy) { //Comparator的实现接口是不能换的
    return Status::InvalidArgument("changing filter policy while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  //(因为BlockBuilders使用的是options的指针,而下面的=赋值不改变地址，只是改变rep_->options的值)
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1; //index block 一定是聚集索引，不可以换
  return Status::OK();
}

//根据需要可能在index block（一个.sst文件只有一个）加入一条记录
//filter add key(可选)
//按增序加入一条记录至data block
//若当前data block的大小到达阈值(默认4K),调用Flush函数 (将改写的写入.sst文件（只是flush到内核）,并reset data block)
//Internal key, value
void MtableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);//按增序插入
  }

  if (r->pending_index_entry) {  //初始为false(表明第一个data block的index不需要显式的存储), 为ture表明.sst文件刚写入一个data block
    assert(r->data_block.empty());
    r->options.comparator->FindShortestSeparator(&r->last_key, key);//第一个参数既为输入也为输出,第一个参数输入输出都应<第二个参数, >=第一个
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);//此值的offset为上一个data block的offset(若上一个为第一个，则offset 为0),size 为上一个data block压缩后的大小
    r->index_block.Add(r->last_key, Slice(handle_encoding)); //index block的一条记录的key>=上一个block的所有key，小于当前插入的key
    r->pending_index_entry = false;
  }
  if (r->filter_block != NULL) {
    r->filter_block->AddKey(key);
  }

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate(); //返回的大小是将data block写入文件的精确大小
  if (estimated_block_size >= r->options.block_size) { //默认为4K(压缩前的大小，若采用了压缩，实际更小),可改变
    Flush();
  }
}


//作用为将data block的内容flush到内核并进行一系列设置使得下次的add操作加入的为下一个data block; 若data block为空立即返回;
  //执行WriteBlock函数
    //生成完整的data block（包括尾部的restart points， num_restarts)
    //对该block根据options是否进行压缩,生成block_contents
    //执行WriteRawBlock函数
      //设置pending_handle(offset1: 当前写的data block的开头 size:不包括最后的type(压缩策略)和crc32，5个字节);
      //在file上执行append block_contents + type + crc32;
      //更新offset1值为文件末尾(可用于设置下一个pending_handle和返回已写文件的大小)
  //reset data block
//pending_index_entry 设置为true(pending_handle 已经在WriteBlock 函数中设置)
//将内存空间的数据flush至内核
//判断是否有filter策略,进行 StartBlock(r->offset)
void MtableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return; //在刚执行完Flush还没有add记录发生
  assert(!r->pending_index_entry); //若为true，data_block为空的,所以不会执行到这
  WriteBlock(&r->data_block, &r->pending_handle, toData);//注意r->appending_handle被设置为写入的data block的开头，若为第一个其offset为0，size为压缩后(若设置了的话)data block 的大小
  if (ok()) {
    r->pending_index_entry = true;
  }
  if (r->filter_block != NULL) {
    r->filter_block->StartBlock(r->offset1 - r->base_offset1);//offset1在此处应看成下一个data block的开头
//    r->filter_block->StartBlock(r->offset1 );//offset1在此处应看成下一个data block的开头
  }
}

//生成完整的(data/index/metaindex) block（包括尾部的restart points, num_restarts)
//对该block根据options是否进行压缩,生成block_contents
//执行WriteRawBlock函数
  //设置pending_handle(offset: 当前写的block的开头 size:不包括最后的type(压缩策略)和crc32，5个字节);
  //在file上执行append block_contents + type + crc32;
  //更新offset值为文件末尾(可用于设置下一个pending_handle和返回已写文件的大小)
//reset (data/index/metaindex) block
void MtableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle, Towhich towhich) { //Towhich为1表示写的是data部分，2表示写索引部分
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish(); //返回完整的一个block的内容给raw,下次要继续add，要先调用Reset()

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: { //进行压缩
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle, towhich);
  r->compressed_output.clear();
  block->Reset();
}

//设置pending_handle(offset 当前写的data block的开头 其size不包括最后的type(压缩策略)和crc32，5个字节);
//pre_data上append block_contents + type + crc32;
//更新offset值为文件末尾(可用于设置下一个pending_handle和返回已写文件的大小)
//对于写到内存的，offset2不更新且只设置handle->size,handle->offset不设置
void MtableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type,
                                 BlockHandle* handle, Towhich towhich) { //Towhich为1表示写入pre_data, Towhich为2表示写入pre_index
  if(towhich == toData) {
    Rep* r = rep_;
    handle->set_offset(r->offset1);//设置pending_handle,offset 初始为0
    handle->set_size(block_contents.size());//size 为当前写的block的大小（不包括尾端的5个字节(看成不属于该block的））
    r->pre_data.append(block_contents.data(), block_contents.size());
    //append type(压缩策略) 和crc校验码 5个字节
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer+1, crc32c::Mask(crc));
    r->pre_data.append(trailer, kBlockTrailerSize);

    r->offset1 += block_contents.size() + kBlockTrailerSize;
  }
  else { //对于写索引部分, handle的offset不设置
    Rep *r = rep_;
    handle->set_size(block_contents.size());//size为当前写的block的大小(不包括尾端的5个字节(看成不属于该block的)), offset不设置
    r->pre_index.append(block_contents.data(), block_contents.size());
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer+1, crc32c::Mask(crc));
    r->pre_index.append(trailer, kBlockTrailerSize);
  }
}

Status MtableBuilder::status() const {
  return rep_->status;
}

//若写不下而不成功不会写磁盘(因为数据部分和索引部分都是先写在string pre_data, pre_index中)
Status MtableBuilder::Finish() {
  Rep* r = rep_;
  Flush(); //作用为将data block的内容flush到内核并进行一系列设置使得下次的add操作加入的为下一个data block; 若data block为空立即返回;
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, index_block_handle; //调用默认构造函数
  std::string& table_index_content = r->pre_index;

  // Write filter block(meta block的一种)
  if (ok() && r->filter_block != NULL) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression, //已经设置为不压缩(而WriteRawBlock也没有压缩功能)
                  &filter_block_handle, toIndex);//将filter block(添加type+crc32)写入文件，更新filter_block_handle ,更新rep_->offset
  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) { //上面的Flush()会设置为ture;该文件没有add任何记录直接调用finish会false(为初始值)
      r->options.comparator->FindShortSuccessor(&r->last_key); //找最短的successor(后继者), key既为输入也为输出
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);//将pending_handle的offset_ , size_的值编码为varint64，并append到*dst
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle, toIndex);//对index_block 进行格式化处理并写入.sst，设置了index_block_handle,用于下面footer使用
  }

   if(r->base_offset2 == 0){ //若整个文件只准备存储1个sorted String, offset2加上一些值以供下面减掉使其等于offset1
     assert(r->base_offset1 == 0);
     r->offset2 = r->offset1 + index_block_handle.size() + kBlockTrailerSize + Footer::kEncodedLength;  
     if(r->filter_block != NULL)
       r->offset2 = r->offset2 + filter_block_handle.size() + kBlockTrailerSize ;
   }

  //设置两种handle
    r->offset2 = r->offset2 - index_block_handle.size() - kBlockTrailerSize - Footer::kEncodedLength;
  index_block_handle.set_offset(r->offset2);
  if(r->filter_block != NULL) {
    r->offset2 = r->offset2 - filter_block_handle.size() - kBlockTrailerSize ;
    filter_block_handle.set_offset(r->offset2);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_filter_handle(filter_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);//footer_encoding 有48bits
    table_index_content.append(footer_encoding);
  }
  //这里需要根据不同的策略，写文件; 或返回空间不足的Status; 或返回一般的错误Status
  if(r->offset2 >= r->offset1 || (r->base_offset1 >= config::fileCommonSize && r->oldHoleSize >= r->pre_index.size())){ //hole is enough(这里可取等于,offset1表示下次从data这里开始写，offset 2表示这次从这里开始写,并不会相互覆盖,故可等于), write all the contents to file
    r->status = r->file->Pwrite(r->base_offset1, r->pre_data);
    assert(r->offset1-r->base_offset1 == r->pre_data.size());
    r->status = r->file->Pwrite(r->offset2, r->pre_index);
  }
  else if(r->oldHoleSize >= r->pre_index.size()) {//可以写得下索引部分(由上可知,offset1等于offset2则表示hole为0,故直接相减即可)write pre_index to file
    //这里的逻辑需要发生改变,因为不能直接append，因为偏移量不是原offset1指示的了, 这里直接返回,写入失败(故对于打开了压缩的,有些性能损失)
    r->status =  Status::FileHoleOnlyHoldIndex("file hole is small that can only hole index, please use fileCommonSize as offset1 and add again");
    r->offset1 = r->base_offset1;
    r->offset2 = r->base_offset2;
    
  }
  else if(r->oldHoleSize < r->pre_index.size()) { //连索引部分也写不下, no write,return status::kFileHoleSmall
    r->status = Status::FileHoleSmall("file hole is too small that can't even store index, please merge them to build a new file");
    r->offset1 = r->base_offset1;
    r->offset2 = r->base_offset2;
  }
  else{
    assert(0);
  }
 
  return r->status;
}



uint64_t MtableBuilder::Index_offset() {
  Rep* r = rep_;
  assert(r->closed);
  return rep_->offset2; //若没有调用finish，而调用的是abandon , offset2未发生改变
  
}

void MtableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
  r->isAbandoned = true;
  r->offset1 = r->base_offset1;
  r->offset2 = r->base_offset2;
}

//即调用了add的函数的次数
uint64_t MtableBuilder::NumEntries() const {
  return rep_->num_entries;
}

//返回offset1，需要调用了Finish或Abandon后调用
uint64_t MtableBuilder::Offset1() const {
  Rep* r = rep_;
  assert(r->closed);
  return r->offset1;
}

//供未Finish时实时调用,返回文件至今已写的大概的大小(不包含索引部分)
uint64_t MtableBuilder::CurrentFileSize() const {
  Rep* r = rep_;
  return r->offset1; //offset1会随着写一个data block而发生一次变化(而index_offset只有在finish之后才变化)
}

//返回本次写的数据的字节，需要调用了Finish或Abandon后调用, includeMeta默认为true
uint64_t MtableBuilder::FileSizeByThisBuilder(bool includeMeta) const {
  Rep* r = rep_;
  assert(r->closed);
  if(includeMeta) {
    return r->pre_data.size() + r->pre_index.size();
  } 
  else {
    return r->pre_data.size();
  }
}
// 调用了Finish或Abandon之后才能调用
uint64_t MtableBuilder::HoleSize() const {
  Rep* r = rep_;
  assert(r->closed);
  //对于正常情况为offset2 - offset1
  //对于SStable其offset2==offset1, 返回的是0, 正确
  //对于只把索引写在洞内的会出现r->offest2 < r->offset1的出现, 此时holeSize减少了写的索引部分
  if(!r->isAbandoned) //成功结束
    return r->offset2 >= r->offset1 ? r->offset2 - r->offset1 : r->oldHoleSize-(r->base_offset2 - r->offset2);
  else //不成功结束
    //对于插入失败的,返回原值(不能应用上面的等式，部分情况下要用base_offset2,而此变量不可得)
    return r->oldHoleSize;
}
// 调用了Finish或Abandon之后才能调用
uint64_t MtableBuilder::IsAbandoned() const {
  Rep* r = rep_;
  assert(r->closed);
  return r->isAbandoned;
}

}  // namespace branchdb
