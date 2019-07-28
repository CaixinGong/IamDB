#include <iostream>
#include <string>
#include "branchdb/db.h"
#include <assert.h>
int main()
{
    std::string dbname = "sunLevel";
    branchdb::Options option;
    option.compression = branchdb::kNoCompression;//关闭压缩，为更好的观看文件结构
    branchdb::DB* dbptr;
    branchdb::Status status = branchdb::DB::Open(option, dbname, &dbptr);
    if(!status.ok()) {//不存在则报错
        std::cout<<"not exist"<<std::endl;
    }   
    assert(status.ok());
    
    std::string value;
    status = dbptr->Get(branchdb::ReadOptions(), branchdb::Slice("2011301500024"), &value);
    if(!status.ok()) {
        std::cout<<"24 not exist"<<std::endl;
    }
    else  
        std::cout<<value<<std::endl;
    status = dbptr->Get(branchdb::ReadOptions(), branchdb::Slice("2011301500021"), &value);
    if(!status.ok()) {
        std::cout<<"21 not exist"<<std::endl;
    }
    else  
        std::cout<<value<<std::endl;
    return 0;
}
