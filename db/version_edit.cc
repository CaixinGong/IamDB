
#include "db/version_edit.h"
#include "db/dbformat.h"
#include "util/coding.h"

namespace branchdb {

//有两种情况，第一种情况为：以前版本中不存在，构造的为新的range，此时addNew为true，rangeBase为NULL;
//            第二种情况为：以前版本中已经存在，构造的为从rangeBase修改而重新得到的range，此时addNew为false，rangeBase为非NULL;
RangeMetaData::RangeMetaData(const RangeInfoForEdit &rangeEdit, bool addNew, const RangeMetaData *rangeBase): refs_(0) {
  assert( (addNew && rangeBase==NULL) ||  (!addNew && rangeBase!=NULL) );
  if(addNew) { //添加的为new range
    //rangeEdit中除了has_old_smallest_,其他的信息都得有(对于非Imm的shift操作,file number同样非空为原值)
    assert(!rangeEdit.has_old_smallest_ && rangeEdit.file_number_ > 0 && \
           rangeEdit.has_smallest_ && rangeEdit.has_largest_ &&  rangeEdit.has_file_number_ && rangeEdit.has_appendTimes_ && \
           rangeEdit.has_index_offset_end_ && rangeEdit.has_index_offset_ && rangeEdit.has_offset1_ && rangeEdit.has_holeSize_ && rangeEdit.has_usePercent_);
    smallest = rangeEdit.smallest_;
    largest = rangeEdit.largest_;

    if(rangeEdit.filemeta == NULL)
      filemeta = new FileMetaData(rangeEdit.file_number_);
    else //只在对于非imm的combine-shift/data-shift时为非空,使得下层复制原来的filemeta新建RangeMetaData
      filemeta = rangeEdit.filemeta;
    assert(filemeta!=NULL);
    filemeta->Ref();

    appendTimes = rangeEdit.appendTimes_;
    index_offset_end = rangeEdit.index_offset_end_;
    index_offset = rangeEdit.index_offset_;
    offset1 = rangeEdit.offset1_;
    holeSize = rangeEdit.holeSize_;
    usePercent = rangeEdit.usePercent_;
  }
  else {       //添加的为modified range
    //rangeEdit中除了has_smallest_,其他的信息都可以不存在，存在的都表示已经改变了
    //若smallest_未改变则old_smallest_为空的,记录在smallest_，若改变old_smallest_和smallest_都不空
    assert(rangeEdit.has_smallest_ && !rangeEdit.filemeta);
    smallest = rangeEdit.smallest_;
    //largest
    if(rangeEdit.has_largest_)
      largest = rangeEdit.largest_;
    else
      largest = rangeBase->largest;
    //filemeta
    if(rangeEdit.has_file_number_) {  //需分配新的文件编号,如某一range被flush之后
      filemeta = new FileMetaData(rangeEdit.file_number_);
      assert(filemeta!=NULL);
    }
    else {
      filemeta = rangeBase->filemeta;
    }
    filemeta->Ref();

    appendTimes = rangeEdit.has_appendTimes_ ? rangeEdit.appendTimes_ : rangeBase->appendTimes;
    index_offset_end = rangeEdit.has_index_offset_end_ ? rangeEdit.index_offset_end_ : rangeBase->index_offset_end;
    index_offset = rangeEdit.has_index_offset_ ? rangeEdit.index_offset_ : rangeBase->index_offset;
    offset1 = rangeEdit.has_offset1_ ? rangeEdit.offset1_ : rangeBase->offset1;
    holeSize = rangeEdit.has_holeSize_ ? rangeEdit.holeSize_ : rangeBase->holeSize;
    usePercent = rangeEdit.has_usePercent_ ? rangeEdit.usePercent_ : rangeBase->usePercent;
  }
  
  if(appendTimes > 0) {
    //情况有多种: 
    //1. flush中的"孩子range",接受了新的数据, 应为, 原值 - 父亲减掉的数值*占分配父亲的数据的分量; 
    //2. 被shift的,应继承原值 
    //3. 被split新生成的,应为, 初始值-父亲的减掉的一半
    //上面需要的父亲的数值,原值等信息,较为复杂，简化处理:appendTimes越大或数据量越大，allowed_seeks越小,见下面具体逻辑
    uint64_t dataMount = DataMount();
    uint64_t dataThreshold = config::fileCommonSize - (config::fileCommonSize>>2);
    if( dataMount >= dataThreshold ) { //数据越多，继承了父亲减掉的值的概率越大,且数据量大flush后不会引入严重的随机性
      allowed_seeks = 0; //下次一旦被读取，则被触发compaction(0等同于1)
    }
    else if( appendTimes >= config::levelTimes ) { //被append的次数多，读的内存中布隆过滤器的判断很多，不利于读性能(需要改成levelTimes/2吗,区别应该不大,因为appendTimes大时，allowed_seeks会比较小)
      allowed_seeks = 0; //下次一旦被读取，则被触发compaction(0等同于1)
    }
    else if(dataMount > 0) {
      /*若平均每16KB*(appendTimes)被读取了一次,则应该被flush掉, appendTimes越多继承了父亲减掉的值的概率越大 且
                                                                被append的次数多，读的内存中布隆过滤器的判断很多，不利于读性能
      假设每个range的孩子数相同，数据量小，但范围一样，可能被读取的概率近似相同,数据量小的被flush的概率需小,这样在混合读取的过程中性能应该会更好*/
      allowed_seeks = ( dataThreshold  - dataMount ) >> (14+appendTimes-1); //无论如何 >= 0
    }
    else {
      assert(0);
      allowed_seeks =  (uint64_t(1)<<63) - 1;
    }

  }
  else { //为发起Adjustment-flush的range或data-shift 因为现在无数据,所以应该设为无穷大，使不应该发起flush以优化读(下次有数据时会被上一个if语句重新设置)
    allowed_seeks =  (uint64_t(1)<<63) - 1; //无穷
  }

}

void RangeInfoForEdit::Clear() {
  has_old_smallest_ = false;
  has_smallest_ = false;
  has_largest_ = false;
  has_file_number_ = false;
  has_appendTimes_ = false;
  has_index_offset_end_ = false;
  has_index_offset_ = false;
  has_offset1_ = false;
  has_holeSize_ = false;
  has_usePercent_ = false;
  filemeta = NULL; //不参与序列化和反序列化
}

enum rTage {
  rOld_smallest = 20,
  rSmallest = 21,
  rLargest = 22,
  rFile_number = 23,
  rAppendTimes = 24,
  rIndex_offset_end = 25,
  rIndex_offset = 26,
  rOffset1 = 27,
  rHoleSize = 28,
  rUsePercent = 29
};

void RangeInfoForEdit::EncodeTo(std::string* dst) const {  //将本对象信息一定格式全部写进*dst中(即序列化)
  if(has_old_smallest_) {
    PutVarint32(dst, rOld_smallest);
    PutLengthPrefixedSlice(dst, old_smallest_.Encode());
  }

  assert(has_smallest_);
  PutVarint32(dst, rSmallest);
  PutLengthPrefixedSlice(dst, smallest_.Encode());

  if(has_largest_) {
    PutVarint32(dst, rLargest);
    PutLengthPrefixedSlice(dst, largest_.Encode());
  }
  if(has_file_number_) {
    PutVarint32(dst, rFile_number);
    PutVarint64(dst, file_number_);
  }
  if(has_appendTimes_) {
    PutVarint32(dst, rAppendTimes);
    PutVarint64(dst, appendTimes_);
  }
  if(has_index_offset_end_) {
    PutVarint32(dst, rIndex_offset_end);
    PutVarint64(dst, index_offset_end_);
  }
  if(has_index_offset_) {
    PutVarint32(dst, rIndex_offset);
    PutVarint64(dst, index_offset_);
  }
  if(has_offset1_) {
    PutVarint32(dst, rOffset1);
    PutVarint64(dst, offset1_);
  }
  if(has_holeSize_) {
    PutVarint32(dst, rHoleSize);
    PutVarint64(dst, holeSize_);
  }
  if(has_usePercent_) {
    PutVarint32(dst, rUsePercent);
    PutVarint64(dst, usePercent_);
  }
    
}

//input前面包含了length + InternalKey，解析完后，*dst为internal key值，input指向length 和 internalKey之后
static bool GetInternalKey(Slice* input, InternalKey* dst) {
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) { 
    dst->DecodeFrom(str); //修改了 *dst
    return true;
  } else {
    return false;
  }
}

Status RangeInfoForEdit::DecodeFrom(const Slice& src) {    //将src中的信息全部提取到本对象中(即反序列化)
  Clear(); //设置为初始状态
  Slice input = src;
  const char* msg = NULL;
  uint32_t tag;
  while (msg == NULL && GetVarint32(&input, &tag)) { //msg==NULL表示至今没有出现解析格式错误
    switch (tag) {
      case rOld_smallest:
        if(GetInternalKey(&input, &old_smallest_)) {
          has_old_smallest_= true;
        }
        else {
          msg = "old smallest internalKey";
        }
        break;
      case rSmallest:
        if(GetInternalKey(&input, &smallest_)) {
          has_smallest_= true;
        }
        else {
          msg = "smallest internalKey";
        }
        break;
      case rLargest:
        if(GetInternalKey(&input, &largest_)) {
          has_largest_= true;
        }
        else {
          msg = "largest internalKey";
        }
        break;
      case rFile_number:
        if (GetVarint64(&input, &file_number_)) {
          has_file_number_ = true;
        }
        else {
          msg = "file number";
        }
        break;
      case rAppendTimes:
        if (GetVarint64(&input, &appendTimes_)) {
          has_appendTimes_ = true;
        }
        else {
          msg = "append times";
        }
        break;
      case rIndex_offset_end:
        if (GetVarint64(&input, &index_offset_end_)) {
          has_index_offset_end_ = true;
        }
        else {
          msg = "index offset end";
        }
        break;
      case rIndex_offset:
        if (GetVarint64(&input, &index_offset_)) {
          has_index_offset_ = true;
        }
        else {
          msg = "index offset";
        }
        break;
      case rOffset1:
        if (GetVarint64(&input, &offset1_)) {
          has_offset1_ = true;
        }
        else {
          msg = "offset1";
        }
        break;
      case rHoleSize:
        if (GetVarint64(&input, &holeSize_)) {
          has_holeSize_= true;
        }
        else {
          msg = "hole size";
        }
        break;
      case rUsePercent:
        if (GetVarint32(&input, &usePercent_)) {
          has_usePercent_ = true;
        }
        else {
          msg = "use Percent";
        }
        break;
      default:
        msg = "unknown tag";
        break;

    }
  }
  
  if (msg == NULL && !input.empty()) {
    msg = "invalid tag";
  }

  Status result;
  if (msg != NULL) {
    result = Status::Corruption("RangeInfoForEdit", msg);
  }
  return result;
}
std::string RangeInfoForEdit::DebugString() const {
  std::string r;
  r.append("range info{");
  if (has_old_smallest_) {
    r.append("\n  Old smallest: ");
    r.append(old_smallest_.DebugString());
  }
  if (has_smallest_) {
    r.append("\n  smallest: ");
    r.append(smallest_.DebugString());
    r.append(" .. ");
  }
  if(has_largest_) {
    r.append("\n  largest: ");
    r.append(largest_.DebugString());
    r.append(" ");
  }
  if(has_file_number_) {
    r.append("\n  file_number: ");
    AppendNumberTo(&r, file_number_);
    r.append(" ");
  }
  if(has_appendTimes_) {
    r.append("\n  append times: ");
    AppendNumberTo(&r, appendTimes_);
    r.append(" ");
  }
  if(has_index_offset_end_) {
    r.append("\n  index offset end: ");
    AppendNumberTo(&r, index_offset_end_);
    r.append(" ");
  }
  if(has_index_offset_) {
    r.append("\n  index offset: ");
    AppendNumberTo(&r, index_offset_);
    r.append(" ");
  }
  if(has_offset1_) {
    r.append("\n  offset1: ");
    AppendNumberTo(&r, offset1_);
    r.append(" ");
  }
  if(has_holeSize_) {
    r.append("\n  holeSize: ");
    AppendNumberTo(&r, holeSize_);
    r.append(" ");
  }
  if(has_usePercent_) {
    r.append("\n  usePercent: ");
    AppendNumberTo(&r, usePercent_);
  }
  return r;
}

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
enum Tag {
  kComparator           = 1,
  kLogNumber            = 2,
  kNextFileNumber       = 3,
  kLastSequence         = 4,
  kDeletedRange         = 5,
  kNewRange             = 6,
  kModifyRange          = 7,
  kPrevLogNumber        = 8
};

void VersionEdit::Clear() {
  comparator_.clear();
  log_number_ = 0;
  prev_log_number_ = 0;
  last_sequence_ = 0;
  next_file_number_ = 0;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_last_sequence_ = false;
  deleted_ranges_.clear();
  new_ranges_.clear();
  modify_ranges_.clear();
}

//将本VersionEdit对象信息一定格式全部写进*dst中(即序列化)
void VersionEdit::EncodeTo(std::string* dst) const {
  if (has_comparator_) { //comparator名
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {//log编号
    PutVarint32(dst, kLogNumber);
    PutVarint64(dst, log_number_);
  }
  if (has_prev_log_number_) {//上一个log编号
    PutVarint32(dst, kPrevLogNumber);
    PutVarint64(dst, prev_log_number_);
  }
  if (has_next_file_number_) { //下一个log编号
    PutVarint32(dst, kNextFileNumber);
    PutVarint64(dst, next_file_number_);
  }
  if (has_last_sequence_) {//上一个序列号
    PutVarint32(dst, kLastSequence);
    PutVarint64(dst, last_sequence_);
  }

  //要删除的range
  for (DeletedRangeSet::const_iterator iter = deleted_ranges_.begin();
       iter != deleted_ranges_.end();
       ++iter) {
    PutVarint32(dst, kDeletedRange);//5
    PutVarint32(dst, iter->first);   // level
    PutLengthPrefixedSlice(dst, iter->second.Encode());  // range  smallest internalKey
  }
  
  //新添加的range
  for (size_t i = 0; i < new_ranges_.size(); i++) {
    const RangeInfoForEdit& r = new_ranges_[i].second;
    PutVarint32(dst, kNewRange);//6
    PutVarint32(dst, new_ranges_[i].first);  // level
    std::string temp;
    r.EncodeTo(&temp);
    PutLengthPrefixedSlice(dst, temp);
  }
  //修改的range
  for (size_t i = 0; i < modify_ranges_.size(); i++) {
    const RangeInfoForEdit& r = modify_ranges_[i].second;
    PutVarint32(dst, kModifyRange);//7
    PutVarint32(dst, modify_ranges_[i].first);  // level
    std::string temp;
    r.EncodeTo(&temp);
    PutLengthPrefixedSlice(dst, temp);
  }
}


//input为level的varint32编码,将input内容提取至level(合法值，返回true), input后移到解析完level 之后
static bool GetLevel(Slice* input, int* level) {
  uint32_t v;
  if (GetVarint32(input, &v) &&
      v < config::kNumLevels) {
    *level = v;
    return true;
  } else {
    return false;
  }
}

//将src中的信息全部提取到本VersionEdit对象中(即反序列化)
Status VersionEdit::DecodeFrom(const Slice& src) {
  Clear(); //设置为初始状态
  Slice input = src;
  const char* msg = NULL;
  uint32_t tag;

  // Temporary storage for parsing
  int level;
  uint64_t number;
  RangeInfoForEdit r;
  Slice str;
  InternalKey key;
  Status result;

  while (msg == NULL && GetVarint32(&input, &tag)) { //msg==NULL表示至今没有出现解析格式错误
    switch (tag) {
      case kComparator:
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;

      case kLogNumber:
        if (GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {
          msg = "log number";
        }
        break;

      case kPrevLogNumber:
        if (GetVarint64(&input, &prev_log_number_)) {
          has_prev_log_number_ = true;
        } else {
          msg = "previous log number";
        }
        break;

      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case kDeletedRange:
        if (GetLevel(&input, &level) &&
            GetInternalKey(&input, &key)) {
          deleted_ranges_.insert(std::make_pair(level, key));
        } else {
          msg = "deleted file";
        }
        break;

      case kNewRange: //修改
        if (GetLevel(&input, &level) &&
            GetLengthPrefixedSlice(&input, &str)) {
          result = r.DecodeFrom(str);
          if(!result.ok()) return result;

          new_ranges_.push_back(std::make_pair(level, r));
        } else {
          msg = "new-file entry";
        }
        break;
      case kModifyRange:  //添加
        if (GetLevel(&input, &level) &&
            GetLengthPrefixedSlice(&input, &str)) {
          result = r.DecodeFrom(str);
          if(!result.ok()) return result;
          modify_ranges_.push_back(std::make_pair(level, r));
        } else {
          msg = "new-file entry";
        }
        break;

      default:
        msg = "unknown tag";
        break;
    }
  }

  if (msg == NULL && !input.empty()) {
    msg = "invalid tag";
  }

  if (msg != NULL) {
    result = Status::Corruption("VersionEdit", msg);
  }
  return result;
}

//以可读的方式将VersionEdit写到字符串
std::string VersionEdit::DebugString() const {
  std::string r;
  r.append("VersionEdit {");
  if (has_comparator_) {
    r.append("\n  Comparator: ");
    r.append(comparator_);
  }
  if (has_log_number_) {
    r.append("\n  LogNumber: ");
    AppendNumberTo(&r, log_number_);
  }
  if (has_prev_log_number_) {
    r.append("\n  PrevLogNumber: ");
    AppendNumberTo(&r, prev_log_number_);
  }
  if (has_next_file_number_) {
    r.append("\n  NextFile: ");
    AppendNumberTo(&r, next_file_number_);
  }
  if (has_last_sequence_) {
    r.append("\n  LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }
  for (DeletedRangeSet::const_iterator iter = deleted_ranges_.begin();
       iter != deleted_ranges_.end();
       ++iter) {
    r.append("\n  DeleteRange: ");
    AppendNumberTo(&r, iter->first);
    r.append(" ");
    r.append(iter->second.DebugString());
  }
  for (size_t i = 0; i < new_ranges_.size(); i++) {
    const RangeInfoForEdit& range = new_ranges_[i].second;
    r.append("\n  AddRange: ");
    AppendNumberTo(&r, new_ranges_[i].first);
    r.append(" ");
    r.append(range.DebugString());
  }
  for (size_t i = 0; i < modify_ranges_.size(); i++) {
    const RangeInfoForEdit& range = modify_ranges_[i].second;
    r.append("\n  ModifyRange: ");
    AppendNumberTo(&r, modify_ranges_[i].first);
    r.append(" ");
    r.append(range.DebugString());
  }
  r.append("\n}\n");
  return r;
}

}  // namespace branchdb
