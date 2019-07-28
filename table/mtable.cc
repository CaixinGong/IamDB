// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "branchdb/mtable.h"

#include "branchdb/cache.h"
#include "branchdb/comparator.h"
#include "branchdb/env.h"
#include "branchdb/filter_policy.h"
#include "table/block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "table/merger.h"
#include "util/coding.h"
#include <iostream>

namespace branchdb {

  //调用析构函数的为被tableCache evict后Handle的引用减为0的
  Mtable::Rep::~Rep() {
    for(int i = 0; i < num_sortedStrings; ++i) {
      if (options.filter_policy != NULL) { //若开启filter
        delete filters[i];
        delete [] filter_datas[i];
      }
      delete index_blocks[i];
    }
  }

//根据.sst文件的Footer，new一个Rep对象，并进行设置其成员，然后用该Rep对象新建一个Table对象并返回
//调用open函数的为没有在tableCache中的
Status Mtable::Open(const Options& options,
                   RandomAccessFile* file,
                   uint64_t index_offset_end,     //索引数据的结束偏移量, 一般为fileCommonSize, 对于只存储一个sorted string的无洞的文件为文件的长度
		   uint64_t index_offset, //索引数据的偏移量，索引数据的大小可以通过index_offset_end推断
                   Mtable** mtable) {

  *mtable = NULL;//打开失败时的值
  uint64_t allSize = index_offset_end - index_offset;
  if (allSize < Footer::kEncodedLength) //即48
    return Status::InvalidArgument("file is too short to be an sstable");

  char *allData = new char[allSize];
  Slice allContent;
  Status s = file->Read(index_offset, allSize,
                        &allContent, allData);
  if (!s.ok()) return s;
  if(allContent.data() != allData) delete[] allData;

  Slice footer_input(allContent.data() + allSize - Footer::kEncodedLength, Footer::kEncodedLength);
  Footer footer;
  s = footer.DecodeFrom(&footer_input);//写Footer对象中的metaindex_handle_ 和 index_handle_
  if (!s.ok()) return s;
  
   
  Rep* rep = new Mtable::Rep;
  rep->options = options;
  rep->file = file;
  rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0); //Block cache,所以cache中的value为Block*
  rep->num_sortedStrings = 0;
  rep->index_offset = index_offset;
  rep->index_offset_end = index_offset_end;
  while(footer.isValid() && footer.index_handle().offset() >= index_offset) {
    rep->num_sortedStrings++;
    //构造index block相关
    BlockContents indexblock_contents;
    s = ReadBlockFromMem(allContent.data(), ReadOptions(), footer.index_handle(), index_offset, &indexblock_contents);
    Block* index_block = new Block(indexblock_contents);
    rep->index_blocks.push_back(index_block); 
    //构造filter相关
    const char *filter_data = NULL;
    FilterBlockReader *filter = NULL;
    if (options.filter_policy != NULL) { //若开启filter
      rep->ReadFilterFromMem(allContent.data(), footer.filter_handle(), index_offset, &filter_data, &filter);
      rep->filters.push_back(filter);
      rep->filter_datas.push_back(filter_data);
    }
    //为解析下一个sorted string作准备
    Footer footer_tmp;
    if(options.filter_policy != NULL && footer.filter_handle().offset() > index_offset) {  //开启了bloom filter
      Slice footer_input(allContent.data() + (footer.filter_handle().offset() - index_offset - Footer::kEncodedLength), Footer::kEncodedLength);
      s = footer_tmp.DecodeFrom(&footer_input);//写Footer对象中的metaindex_handle_ 和 index_handle_
    }
    else if(options.filter_policy == NULL && footer.index_handle().offset() > index_offset){  //未开启bloom filter
      Slice footer_input(allContent.data() + (footer.index_handle().offset() - index_offset - Footer::kEncodedLength), Footer::kEncodedLength);
      s = footer_tmp.DecodeFrom(&footer_input);//写Footer对象中的metaindex_handle_ 和 index_handle_
    }
    footer = footer_tmp;

    if (!s.ok()) {
      if(allContent.data() == allData) delete[] allData;
      return s;
    }
  }

  if(allContent.data() == allData) delete[] allData;
  *mtable = new Mtable(rep);

  return s;
}
Status Mtable::Reopen(const Options& options,
                   RandomAccessFile* file,
                   uint64_t cache_index_offset,
                   uint64_t index_offset_end,  //索引数据的结束偏移量, 一般为fileCommonSize, 对于只存储一个sorted string的无洞的文件为文件的长度
	    	   uint64_t index_offset,      //索引数据的偏移量，索引数据的大小可以通过index_offset_end推断
		   const Mtable* mtable,
                   Mtable** newMtable) {

  *newMtable = NULL;//打开失败时的值
  Status s;
  if(cache_index_offset == index_offset)  return s;
  assert(cache_index_offset > index_offset);

  uint64_t allSize = cache_index_offset - index_offset;
  if (allSize < Footer::kEncodedLength) //即48
    return Status::InvalidArgument("the append sorted string(s) is too short");

  char *allData = new char[allSize];
  assert(allData!=NULL);
  Slice allContent;
  s = file->Read(index_offset, allSize,
                        &allContent, allData);
  if (!s.ok()) return s;
  if(allContent.data() != allData) delete[] allData;

  Slice footer_input(allContent.data() + allSize - Footer::kEncodedLength, Footer::kEncodedLength);
  Footer footer;
  s = footer.DecodeFrom(&footer_input);//写Footer对象中的metaindex_handle_ 和 index_handle_
  if (!s.ok()) return s;
  
   
  Rep* rep = new Mtable::Rep;
  rep->options = options;
  assert(options.filter_policy == mtable->rep_->options.filter_policy); //不允许变化
  rep->file = file;
  rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0); //Block cache,所以cache中的value为Block*
  rep->index_offset = index_offset;
  rep->index_offset_end = index_offset_end;
  rep->num_sortedStrings = mtable->rep_->num_sortedStrings;
  for(int i = 0; i < rep->num_sortedStrings; ++i ){ //复制filter和index数据, 其在vector的顺序不改变
    //索引的数据都是从堆中分配的(OPT? 改完cache功能后，再改动)
   
    //1.构造index block content 
    BlockContents indexblock_contents;
    const char *source_data = mtable->rep_->index_blocks[i]->data_;
    size_t size = mtable->rep_->index_blocks[i]->size_;
    char *data = new char[size];
    memcpy(data, source_data, size);
    indexblock_contents.data = Slice(data, size);    
    indexblock_contents.cachable = true;
    indexblock_contents.heap_allocated = true;
    //2.构造index  block并push_back至index_blocks
    Block* index_block = new Block(indexblock_contents);
    rep->index_blocks.push_back(index_block); 

    if (options.filter_policy != NULL) { //若开启filter
      //3.构造filter_data
      char *filter_data = NULL;
      const char *source_filter_data = mtable->rep_->filter_datas[i];
      size_t size = mtable->rep_->filters[i]->size(); 
      filter_data = new char[size];
      memcpy(filter_data, source_filter_data, size);
      rep->filter_datas.push_back(filter_data);
      //4.构造filter
      Slice slice_filter(filter_data, size);
      FilterBlockReader* filter = new FilterBlockReader(options.filter_policy, slice_filter);
      rep->filters.push_back(filter);
    }
  }
  while(footer.isValid() && footer.index_handle().offset() >= index_offset) {
    rep->num_sortedStrings++;
    //构造index block相关
    BlockContents indexblock_contents;
    s = ReadBlockFromMem(allContent.data(), ReadOptions(), footer.index_handle(), index_offset, &indexblock_contents);
    Block* index_block = new Block(indexblock_contents);
    rep->index_blocks.push_back(index_block); 
    //构造filter相关
    const char *filter_data = NULL;
    FilterBlockReader *filter = NULL;
    if (options.filter_policy != NULL) { //若开启filter
      rep->ReadFilterFromMem(allContent.data(), footer.filter_handle(), index_offset, &filter_data, &filter);
      rep->filters.push_back(filter);
      rep->filter_datas.push_back(filter_data);
    }
    //为解析下一个sorted string作准备
    Footer footer_tmp;
    if(options.filter_policy != NULL && footer.filter_handle().offset() > index_offset) {  //开启了bloom filter
      Slice footer_input(allContent.data() + (footer.filter_handle().offset() - index_offset - Footer::kEncodedLength), Footer::kEncodedLength);
      s = footer_tmp.DecodeFrom(&footer_input);//写Footer对象中的metaindex_handle_ 和 index_handle_
    }
    else if(options.filter_policy == NULL && footer.index_handle().offset() > index_offset){  //未开启bloom filter
      Slice footer_input(allContent.data() + (footer.index_handle().offset() - index_offset - Footer::kEncodedLength), Footer::kEncodedLength);
      s = footer_tmp.DecodeFrom(&footer_input);//写Footer对象中的metaindex_handle_ 和 index_handle_
    }
    footer = footer_tmp;

    if (!s.ok()) {
      if(allContent.data() == allData) delete[] allData;
      return s;
    }
  }

  if(allContent.data() == allData) delete[] allData;
  *newMtable = new Mtable(rep);

  return s;
}

//filter_data一定会被设置(从堆中分配)
void Mtable::Rep::ReadFilterFromMem(const char* allData, const BlockHandle& filter_handle, uint64_t index_offset, const char **filter_data, FilterBlockReader **filter) {

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  BlockContents block;
  if (!ReadBlockFromMem(allData, opt, filter_handle, index_offset, &block).ok()) {//读取meta block的内容至block，且其cachable,heap_allocated 两个bool变量被设置
    return;
  }
  if (block.heap_allocated) { //当block中的data需要被后面被用户deltete才会设置filter_data
    *filter_data = block.data.data();     // Will need to delete later
  }
  *filter = new FilterBlockReader(options.filter_policy, block.data); //FilterBlockReader的对象用于读filter
}

Mtable::~Mtable() {
  delete rep_;
}

//当未采用cache block策略时，用于回收内存（对应的迭代器析构时）
static void DeleteBlock(void* arg, void* ignored) { //第二个参数只是用来满足同一函数原型，以被函数指针指向
  delete reinterpret_cast<Block*>(arg);
}

//采用cache block策略时 在cache中的一个元素要从cache中剔除时，key(这里不需要回收)，value(这里为Block *)的内存回收
static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

//采用cache block策略时当不用一个Handle* 时，需要Release该Handle*（对应的迭代器析构时）
static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value即index_value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// 私有函数,arg可以为Table*类型的值如this,见InternalGet函数或NewIterator函数的使用本函数
// 参数index_value为要为索引的block的BlockHandle的offset_和size_的varint64编码
// 功能：从index_value即在index block获得的BlockHandle的编码构造指向的data block的迭代器,若开启block cache选项，先查options.block_cache，若无,从磁盘读取并插入该block_cache(该cache只为data block cache)
// 关于data block的cache插入读取等的使用都在这
Iterator* Mtable::BlockReader(void* arg,
                             const ReadOptions& options,
                             const Slice& index_value) { //index_value为 从index block获得的BlockHandle
  Mtable* table = reinterpret_cast<Mtable*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = NULL;
  Cache::Handle* cache_handle = NULL;//Handle 隐藏了实现相关的细节

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);//将input中的varint64的两个值 解码到handle的offset_ , size_ ,并且input中的指针前移
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.
  
  //获得该Block* block
  if (s.ok()) {
    BlockContents contents;
    if (block_cache != NULL) { //若采用了cache block的机制
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id); //key为cache_id(每个Table::Open生成的table对象都相同)+ block offset
      EncodeFixed64(cache_key_buffer+8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key); //不在使用找到的内容后，需要Release
      if (cache_handle != NULL) {     //cache中找到
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle)); //value为 Block *
      } else {                        //cache中未找到
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) { 
          block = new Block(contents); //对象的析构在DeleteCacheBlock中
          if (contents.cachable && options.fill_cache) { 
            cache_handle = block_cache->Insert(
                key, block, block->size(), &DeleteCachedBlock);//插入cache中。DeleteCachedBlock用于传递给cache内部进行 key，value的内存回收
                                                               //其他三个参数分别为: const Slice& key, void* value, size_t charge
															   //实际占用的内存空间大于block->size(),但是逻辑上以该值为charge
          }
        }
      }
    } else {                 //未采用cache block的机制
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  //获得迭代器
  Iterator* iter;
  if (block != NULL) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == NULL) { //未采用cache block的机制
      iter->RegisterCleanup(&DeleteBlock, block, NULL); //注册函数：当iter析构(纯虚基类析构前调用)时，delete block
    } else {                    //采用cache block的机制:Block *存储于cache中，用户只需Release cache_handle，真正的删除在cache内部进行
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle); //注册函数：当iter析构(纯虚基类析构前调用)时，release cache_handle
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator* Mtable::NewIterator(const ReadOptions& options, uint64_t appendTimes) const {
  assert(appendTimes <= rep_->num_sortedStrings);
  Iterator* *Multi_iterators = new Iterator*[appendTimes];
  for(int i =0; i < appendTimes; ++i) {
    Multi_iterators[i] =  NewTwoLevelIterator(
		       rep_->index_blocks[i]->NewIterator(rep_->options.comparator),
		       &Mtable::BlockReader, const_cast<Mtable*>(this), options);
  }
  Iterator* result = NewMergingIterator(
                   rep_->options.comparator, Multi_iterators, appendTimes);
  
  delete [] Multi_iterators;
  return result;
}


//私有函数，供友元类TableCache调用
//利用rep_->index_block找到相应data block的BlockHandle,然后用上面的BlockReader函数获得该data block的迭代器
//利用迭代器获得>=该key（参数k）的key/value对，传递给saver函数:(*saver)(arg, block_iter->key(), block_iter->value());
Status Mtable::InternalGet(const ReadOptions& options, const Slice& k, uint64_t appendTimes,
                          void* arg,
                          bool (*saver)(void*, const Slice&, const Slice&)) {
  Status s;
  assert(appendTimes <= rep_->num_sortedStrings);
  for(int64_t i = appendTimes-1; i >= 0; --i) {
    bool founded = false;
    Iterator* iiter = rep_->index_blocks[i]->NewIterator(rep_->options.comparator);
    iiter->Seek(k);//搜索到第一个key>=k的记录
    if (iiter->Valid()) {
      Slice handle_value = iiter->value();//index block 的格式为key(>= last key in that data block), value(offset_, size_ 的varint64编码形式)
      Slice handle_value_save = iiter->value();//index block 的格式为key(>= last key in that data block), value(offset_, size_ 的varint64编码形式)
      FilterBlockReader* filter = NULL;
      if(rep_->filters.size() > 0) {
        assert( i < rep_->filters.size() );
        filter = rep_->filters[i];
      }
      BlockHandle handle;

      iiter->SeekToFirst();
      Slice handle_value_base = iiter->value();//index block 的格式为key(>= last key in that data block), value(offset_, size_ 的varint64编码形式)
      BlockHandle handleBase;
      Status stmp = handleBase.DecodeFrom(&handle_value_base);
      if(!stmp.ok()) break; 

      if (filter != NULL &&
          handle.DecodeFrom(&handle_value).ok() &&
//          !filter->KeyMayMatch(handle.offset(), k)) { //filter_block.cc的函数，对internal key处理
          !filter->KeyMayMatch(handle.offset() - handleBase.offset(), k)) { // 需要减去后面的base_offset1(见mtable_builder.cc中对StartBlock函数的调用)
        // Not found
      } else { //未采用fiter策略 或 采用filter策略并且这个block可能包含该key 则都该block
        Iterator* block_iter = BlockReader(this, options, handle_value_save);//获得data block的迭代器
        block_iter->Seek(k);//搜索到第一个key>=k的记录
        if (block_iter->Valid()) {
          founded = (*saver)(arg, block_iter->key(), block_iter->value());
        }
        s = block_iter->status();
        delete block_iter;
      }
    }
    if (s.ok()) {
      s = iiter->status();
    }
    delete iiter;
    if (founded) break;
  }
  return s;
}

}  // namespace branchdb
