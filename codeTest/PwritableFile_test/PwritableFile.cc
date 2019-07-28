#include <iostream>
#include "branchdb/slice.h"
#include "branchdb/env.h"
#include <assert.h>

int main() {
  branchdb::Env* env =  branchdb::Env::Default();

  branchdb::PwritableFile* pwf;
  branchdb::Status s = env->NewPwritableFile("./file1", &pwf);
  assert(s.ok());

  pwf->Pwrite(5, branchdb::Slice("BBBB\n") );
  pwf->Pwrite(15, branchdb::Slice("DDDD\n") );

//实验验证正确
  pwf->Pwrite(20,branchdb::Slice("EEEE\n") );
  pwf->Pwrite(25,branchdb::Slice("FFFF\n") );
  pwf->Pwrite(95, branchdb::Slice("ZZZZ\n") );
  pwf->Pwrite(90, branchdb::Slice("XXXX\n") );

  pwf->Sync();
  delete pwf;

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
