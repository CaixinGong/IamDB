#ifndef STORAGE_BRANCHDB_DB_VERSION_EDIT_H_
#define STORAGE_BRANCHDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"
#include "port/port.h"

namespace branchdb {

class VersionSet;
class RangeInfoForEdit;

//下面两个类为内存中组织的
struct FileMetaData {
  FileMetaData(uint64_t file_num) : refs_(0), file_number(file_num) { }  //range_lock调用默认构造函数
  void Ref() { ++refs_; }
  void Unref() { 
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }
  //默认的合成析构函数
  const uint64_t file_number;        //与文件绑定的唯一的编号, 若为0，表示还未绑定
  port::Mutex range_lock;            //range和file是一一对应关系，range lock即file lock
 private:
  int refs_;
  ~FileMetaData() {assert(refs_<=0);}  //析构函数是私有的，什么也不做，删除文件的工作交给DBImpl类
};


struct RangeMetaData {
  InternalKey smallest;       // Smallest internal key served by table
  InternalKey largest;        // Largest internal key served by table
  FileMetaData *filemeta;
  uint64_t appendTimes;       //sorted strings的个数, 实际没什么用,但是还是维持了这个信息
  uint64_t index_offset_end;  //索引数据的结束偏移量, 一般为fileCommonSize, 对于只存储一个sorted string的无洞的文件为"文件的长度"
  uint64_t index_offset;      //索引数据的偏移量,也称作offset2， 索引数据的大小可以和index_offset_end一起得出
  uint64_t offset1;           //实际数据的偏移量
  uint64_t holeSize;          //文件洞的大小(由MtableBuilder::HoleSize()赋值的)
  uint32_t usePercent;   //对于MSStable文件,若没有超过一般长度为"实际大小/fileCommonSize",否则为100(%),对于SStable为100(%)

  //不参与序列化和反序列化
  int64_t allowed_seeks;    // Seeks allowed until compaction，使用时设置为file_size/16KB，原因见version_edit.cc的Apply函数

  RangeMetaData() : refs_(0) { }
  RangeMetaData(const RangeInfoForEdit &rangeEdit, bool addNew, const RangeMetaData *rangeBase);
  ~RangeMetaData() {
    assert(refs_ == 0 && filemeta != NULL);
    filemeta->Unref();  //目前对应的文件是LogAndApply成功后统一释放(DeleteObsoleteFiles函数中),这里并不进行
  }
  bool refZeroDelete() {
    if (refs_ == 0) {
	  delete this;//对于refs初始为0的，而最后又没有被使用而引用+1的使用本函数析构
    }
  } 
  //max(offset1, index_offset_end)可以作为文件的file size
  uint64_t file_size() const { //注意"不"等同于有多少数据量
    return offset1 < index_offset_end ? index_offset_end : offset1;
  }
  uint64_t DataMount(bool includeMeta = true) const { //存储了多少数据量,若includeMeta为ture表示包括元数据的所有空间,若为false表示不包括元数据的空间之和
    assert(usePercent == 0 && file_size() - holeSize == 0 || file_size() - holeSize > 0);
    if(includeMeta)
      return file_size() - holeSize;
    else {
      return offset1 < index_offset_end ? offset1 : offset1 - index_offset_end + index_offset - holeSize;
    }
  }
  bool isFull() const {
    assert(usePercent >=0 && usePercent <= 100);
    return usePercent >= config::kfullUsePercent; //默认为95
  }
  void Ref() {++refs_;}
  void Unref() {
    assert(refs_ >= 1);
    --refs_;
    if (refs_ == 0) {
      delete this;
    }
  }
 private:
  int refs_;
  
};


//用于持久化到磁盘的信息
class RangeInfoForEdit {
 public:
  RangeInfoForEdit() {Clear();}
  //通过一个以前版本中存在的range构造新的; 所有信息都存在除了old_smallest_都被赋值
  //本函数只适用于非Imm的shift(combine-shift或data-shit):复制要shift的range。因为会记录原来的filemeta而不是新建;
  RangeInfoForEdit(const RangeMetaData& newr) {
    Clear();
    Set_smallest(newr.smallest);
    Set_largest(newr.largest);
    assert(newr.filemeta && newr.filemeta->file_number);
    Set_file_number(newr.filemeta->file_number);
    Set_appendTimes(newr.appendTimes);
    Set_index_offset_end(newr.index_offset_end);
    Set_index_offset(newr.index_offset);
    Set_offset1(newr.offset1);
    Set_holeSize(newr.holeSize);
    Set_usePercent(newr.usePercent);
    filemeta = newr.filemeta; //指向原来的filemeta
  }
  //本成员只在对于combine-shift/data-shift时为非空,使得下层复制原来的filemeta新建RangeMetaData,以使得每个文件(或说file number)有且
  //只有一个filemeta与之对应; 本成员不参与序列化和反序列化,即不写MANIFEST文件进行持久化
  FileMetaData* filemeta; //重命名为"shift_filemeta"更好

  void Clear();

  void Set_old_smallest(const InternalKey& old_smallest) {
    has_old_smallest_ = true;
    old_smallest_ = old_smallest;
  }
  void Set_smallest(const InternalKey& smallest) {
    has_smallest_ = true;
    smallest_ = smallest;
  }
  const InternalKey& Get_smallest() const {
    assert(has_smallest_);
    return smallest_;
  }
  void Set_largest(const InternalKey& largest) {
    has_largest_= true;
    largest_= largest;
  }
  InternalKey Get_largest() const {
    if(has_largest_)
      return largest_;
    else
      return InternalKey();
  }
  void Set_file_number(uint64_t file_number) {
    has_file_number_= true;
    file_number_= file_number;
  }
  int64_t Get_file_number() const {
    if(has_file_number_)
      return file_number_;
    else
      return -1;
  }
  void Set_appendTimes(uint64_t appendTimes) {
    has_appendTimes_ = true;
    appendTimes_ = appendTimes;
  }
  int64_t Get_appendTimes() const {
    if(has_appendTimes_) 
      return appendTimes_;
    else
      return -1;
  }
  void Set_index_offset_end(uint64_t index_offset_end) {
    has_index_offset_end_= true;
    index_offset_end_= index_offset_end;
  }
  int64_t Get_index_offset_end() const {
    if(has_index_offset_end_) 
      return index_offset_end_;
    else
      return -1;
  }
  void Set_index_offset(uint64_t index_offset) {
    has_index_offset_= true;
    index_offset_= index_offset;
  }
  int64_t Get_index_offset() const {
    if(has_index_offset_)
      return index_offset_;
    else
      return -1;
  }
  void Set_offset1(uint64_t offset1) {
    has_offset1_= true;
    offset1_= offset1;
  }
  int64_t Get_offset1() const {
    if(has_offset1_)
      return offset1_;
    else
      return -1;
  }
  void Set_holeSize(uint64_t holeSize) {
    has_holeSize_= true;
    holeSize_ = holeSize;
  }
  int64_t Get_holeSize() const {
    if(has_holeSize_)
      return holeSize_;
    else
      return -1;
  }
  void Set_usePercent(uint32_t usePercent) {
    has_usePercent_= true;
    usePercent_= usePercent;
  }
  //需要设置了holeSize_, index_offset_, offset1才可调用
  void Set_usePercentByCompute() {
    assert(has_holeSize_ && has_index_offset_ && has_offset1_);
    has_usePercent_= true;
    if(holeSize_ == 0) { //对于单个sorted String 或 恰好用完空间的
      usePercent_ = 100;
    }
    else if( offset1_ <= index_offset_) { //正常情况
      usePercent_ = 100 - (index_offset_ - offset1_)*100/config::fileCommonSize; //使用的比例向上取整
    }
    else    //对于offset1 > index_offset_,即已经有数据写到洞外(虽然holeSize_!=0，也设置为100)
      usePercent_ = 100;
  }
  bool isFull() const {
    assert(has_usePercent_ && usePercent_ >=0 && usePercent_ <= 100);
    return usePercent_ >= config::kfullUsePercent; //默认为95
    
  }
   
  void EncodeTo(std::string* dst) const;  //将本对象信息一定格式全部写进*dst中(即序列化)
  Status DecodeFrom(const Slice& src);    //将src中的信息全部提取到本对象中(即反序列化)

  //返回要在原来版本中搜索的InternalKey以定位原来的Range(在VersionSet::Builder中使用以找到原来的RangeMetaData)
  const InternalKey& SearchKey() const {
    assert(has_smallest_);
    return has_old_smallest_ ? old_smallest_ : smallest_;
  }
  //以可读的方式将RangeInfoForEdit写到字符串
  std::string DebugString() const;

 private: 
  friend class RangeMetaData;

//记录的是RangeMetaData的变化信息(smallest_可能例外,见下面注释)

  InternalKey old_smallest_;   // old Smallest internal key,用于定位要修改的range; 若未改变则为空的,记录在smallest_，若改变old_smallest_和smallest_都不空
                                   //即需满足assert(has_smallest_ || (has_old_smallest_ && has_smallest_) ); 即assert(has_smallest_);
  InternalKey smallest_;       // Smallest internal key served by table
  InternalKey largest_;        // Largest internal key served by table
  uint64_t file_number_;            //指得是file number, 唯一的编号
  uint64_t appendTimes_;       //sorted strings的个数
  uint64_t index_offset_end_;  //索引数据的结束偏移量, 一般为fileCommonSize, 对于只存储一个sorted string的无洞的文件为文件的长度
  uint64_t index_offset_;      //索引数据的偏移量,也称作offset2， 索引数据的大小可以通过index_offset_end推断
  uint64_t offset1_;           //实际数据的偏移量
  uint64_t holeSize_;          //文件洞的大小
  uint32_t usePercent_;   //对于MSStable文件,若没有超过一般长度为"实际大小/fileCommonSize",否则为100(%),对于SStable为100(%)

  bool has_old_smallest_;
  bool has_smallest_;
  bool has_largest_;
  bool has_file_number_;
  bool has_appendTimes_;
  bool has_index_offset_end_;
  bool has_index_offset_;
  bool has_offset1_;
  bool has_holeSize_;
  bool has_usePercent_;
};


class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() { }

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
  void AddNewRange(int level, const RangeInfoForEdit &r) {
      new_ranges_.push_back(std::make_pair(level, r));
  }

  void AddModifyRange(int level, const RangeInfoForEdit &r) {
      modify_ranges_.push_back(std::make_pair(level, r));
  }

  // Delete the specified "range" from the specified "level".
  void DeleteRange(int level, const InternalKey& smallest) {
    deleted_ranges_.insert(std::make_pair(level, smallest));
  }

  void EncodeTo(std::string* dst) const;  //将本VersionEdit对象信息一定格式全部写进*dst中(即序列化)
  Status DecodeFrom(const Slice& src);    //将src中的信息全部提取到本VersionEdit对象中(即反序列化)

  //以可读的方式将VersionEdit写到字符串
  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set< std::pair<int, InternalKey> > DeletedRangeSet; //<level, smallest>,注意为set中的元素不能重复

  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_; //初始时为2 见db_impl.cc的NewDB()函数
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  DeletedRangeSet deleted_ranges_; //被删除的range信息
  std::vector< std::pair<int,RangeInfoForEdit> > new_ranges_; //<level, RangeInfoForEdit(信息较详细)>
  std::vector< std::pair<int,RangeInfoForEdit> > modify_ranges_; //<level, RangeInfoForEdit(信息较详细)>, 记录的是修改range的信息(记录本信息而不与上面两个合并是为了减少log所需的信息)
};

}  // namespace branchdb

#endif  // STORAGE_BRANCHDB_DB_VERSION_EDIT_H_
