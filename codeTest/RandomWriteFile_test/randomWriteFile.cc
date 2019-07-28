#include <iostream>
#include "branchdb/slice.h"
#include "branchdb/env.h"
#include <assert.h>

int main() {
  branchdb::Env* env =  branchdb::Env::Default();

  branchdb::RandomWritableFile* frw;
  branchdb::Status s = env->NewRandomWritableFile("./file1", &frw);
  assert(s.ok());

  frw->Seek(5);
  frw->Write(branchdb::Slice("BBBB\n") );
  frw->Seek(15);
  frw->Write(branchdb::Slice("DDDD\n") );

//实验验证正确
  frw->Write(branchdb::Slice("EEEE\n") );
  frw->Write(branchdb::Slice("FFFF\n") );
  frw->Seek(95);
  frw->Write(branchdb::Slice("ZZZZ\n") );
  frw->Seek(90);
  frw->Write(branchdb::Slice("XXXX\n") );
  
  frw->Flush();

  frw->Sync();
  delete frw;

//验证可以同时读未改变的内容


  branchdb::RandomAccessFile* frr;
  branchdb::Slice result;
  char scratch[5];
  s = env->NewRandomAccessFile("./file1", &frr);
  frr->Read(5, 5, &result, scratch);
  std::cout << result.ToString() <<std::endl; 
  frr->Read(90, 5, &result, scratch);
  std::cout << result.ToString() <<std::endl; 
  delete frr;


  return 0;
}
