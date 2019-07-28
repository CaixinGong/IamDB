#include <iostream>
#include <string>
#include "branchdb/db.h"
#include "branchdb/filter_policy.h"
#include <assert.h>

int main()
{
    std::string dbname = "sunLevel";
    branchdb::Options option;
    option.create_if_missing = true;
    option.error_if_exists = true;//若存在，则返回时报错
    option.compression = branchdb::kNoCompression;//关闭压缩，为更好的观看文件结构
    option.filter_policy = branchdb::NewBloomFilterPolicy(10);

//    option.write_buffer_size =  64*1024*1024;
//    option.block_size = 64*1024;

    branchdb::DB* dbptr;
    branchdb::Status status = branchdb::DB::Open(option, dbname, &dbptr);
    if(!status.ok()) {//先假设这里是因为已存在数据库（虽然不严谨）
        std::cout<<"delete the old"<<std::endl;
        branchdb::DestroyDB(dbname, option);
        status = branchdb::DB::Open(option, dbname, &dbptr);
    }

    //插入
    assert(status.ok());
    status = dbptr->Put(branchdb::WriteOptions(),branchdb::Slice("2011301500024"), branchdb::Slice("caixinGong"));
    assert(status.ok());
    status = dbptr->Put(branchdb::WriteOptions(),branchdb::Slice("2011301500021"), branchdb::Slice("xiezhidong"));
    assert(status.ok());
    status = dbptr->Put(branchdb::WriteOptions(),branchdb::Slice("2011301500022"), branchdb::Slice("wuxiaoming"));
    assert(status.ok());
    status = dbptr->Put(branchdb::WriteOptions(),branchdb::Slice("2011301500023"), branchdb::Slice("sunbinfei"));
    assert(status.ok());
    //查询
    std::string value;
    status = dbptr->Get(branchdb::ReadOptions(), branchdb::Slice("2011301500024"), &value);
    assert(status.ok());
    std::cout<<value<<std::endl;
    status = dbptr->Get(branchdb::ReadOptions(), branchdb::Slice("2011301500021"), &value);
    assert(status.ok());
    std::cout<<value<<std::endl;
    //删除
    status = dbptr->Delete(branchdb::WriteOptions(), branchdb::Slice("2011301500024")); 
    //迭代
    branchdb::Iterator * iterDB = dbptr->NewIterator(branchdb::ReadOptions());
    iterDB->SeekToFirst();
    for(; iterDB->Valid(); iterDB->Next() ) {
      std::cout<< "key: " << iterDB->key().ToString() << ", value: " << iterDB->value().ToString() <<std::endl;
    }
    delete iterDB;

    delete dbptr;
    return 0;
}
//回收env的资源 



