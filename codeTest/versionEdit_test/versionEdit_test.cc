#include <stdlib.h>
#include <iostream>
#include "db/version_edit.h"

//验证edit序列化后再反序列化再序列化中两次序列化得到的字符串完全相同
static void TestEncodeDecode(const branchdb::VersionEdit& edit) {
  std::string encoded, encoded2;
  edit.EncodeTo(&encoded);
  branchdb::VersionEdit parsed;
  branchdb::Status s = parsed.DecodeFrom(encoded);
  assert(s.ok());
  parsed.EncodeTo(&encoded2);
  assert(encoded == encoded2);
}

int main() {
//  static const uint64_t kBig = 1ull << 50;
  static const uint64_t kBig = 0;

  branchdb::VersionEdit edit;
  for (int i = 0; i < 4; i++) {
    TestEncodeDecode(edit);
    
    branchdb::RangeInfoForEdit r;
    r.Set_old_smallest( branchdb::InternalKey("foo", kBig + 100 + i, branchdb::kTypeValue) );
    r.Set_smallest( branchdb::InternalKey("foo", kBig + 200 + i, branchdb::kTypeValue) );
    r.Set_largest( branchdb::InternalKey("zoo", kBig + 300 + i, branchdb::kTypeDeletion) );
    r.Set_file_number(kBig + 400 + i);
    r.Set_appendTimes(kBig + 500 + i);
    r.Set_index_offset_end(kBig + 600 + i);
    r.Set_index_offset(kBig + 700 + i);
    r.Set_offset1(kBig + 800 + i);
    r.Set_holeSize(kBig + 900 + i);
    r.Set_usePercent(kBig + 1000 + i);
    edit.AddNewRange(i, r); //增加的新文件
  
    
    r.Clear();
  
    r.Set_old_smallest( branchdb::InternalKey("foo", kBig + 100 + i, branchdb::kTypeValue) );
    r.Set_smallest( branchdb::InternalKey("foo", kBig + 200 + i, branchdb::kTypeValue) );
    r.Set_appendTimes(kBig + 500 + i);
    edit.AddModifyRange(i, r); //添加修改的新文件

    edit.DeleteRange(i, branchdb::InternalKey("foo", kBig + 700 + i, branchdb::kTypeValue));
  }

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetPrevLogNumber(kBig+500);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);

  TestEncodeDecode(edit);

  std::cout << edit.DebugString() << std::endl;
  std::cout << "pass test!"  << std::endl;

}
