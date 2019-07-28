// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <ctype.h>
#include <stdio.h>
#include "db/filename.h"
#include "db/dbformat.h"
#include "branchdb/env.h"
#include "util/logging.h"

namespace branchdb {

// A utility routine: write "data" to the named file and Sync() it.
extern Status WriteStringToFileSync(Env* env, const Slice& data,
                                    const std::string& fname);

static std::string MakeFileName(const std::string& name, uint64_t number,
                                const char* suffix) {
  char buf[100];
  snprintf(buf, sizeof(buf), "/%06llu.%s",  //注意前面有"/" 所以name+buf生成一个相对路径名
           static_cast<unsigned long long>(number),
           suffix);
  return name + buf;
}

//log的文件名，如SunLevel/000003.log
std::string LogFileName(const std::string& name, uint64_t number) {
  assert(number > 0);
  return MakeFileName(name, number, "log");
}

//MSSTable的文件名 如SunLevel/000005.bdb
std::string MtableFileName(const std::string& dbname, uint64_t number) {
  assert(number > 0);
  return MakeFileName(dbname, number, "bdb");
}

//生成Manifest文件的文件名 如sunLevel/MANIFEST-000002
std::string DescriptorFileName(const std::string& dbname, uint64_t number) {
  assert(number > 0);
  char buf[100];
  snprintf(buf, sizeof(buf), "/MANIFEST-%06llu",
           static_cast<unsigned long long>(number));
  return dbname + buf;
}

//生成CURRENT文件名，如sunLevel/CURRENT
std::string CurrentFileName(const std::string& dbname) {
  return dbname + "/CURRENT";
}

//生成LOCK文件名，如sunLevel/LOCK
std::string LockFileName(const std::string& dbname) {
  return dbname + "/LOCK";
}

//格式为 如： sunLevel/000002.dbtmp
std::string TempFileName(const std::string& dbname, uint64_t number) {
  assert(number > 0);
  return MakeFileName(dbname, number, "dbtmp");
}

//生成LOG文件名 如: sunLevel/LOG
std::string InfoLogFileName(const std::string& dbname) {
  return dbname + "/LOG";
}

// Return the name of the old info log file for "dbname".
// 生成LOG.old文件名 如: sunLevel/LOG.old
std::string OldInfoLogFileName(const std::string& dbname) {
  return dbname + "/LOG.old";
}

//根据fname解析出number(下面注释末未加整数的返回0)，和type
// Owned filenames have the form:
//    dbname/CURRENT
//    dbname/LOCK
//    dbname/LOG
//    dbname/LOG.old
//    dbname/MANIFEST-[0-9]+
//    dbname/[0-9]+.(log|sst|ldb)
bool ParseFileName(const std::string& fname,
                   uint64_t* number,
                   FileType* type) {
  Slice rest(fname);
  if (rest == "CURRENT") {
    *number = 0;
    *type = kCurrentFile;
  } else if (rest == "LOCK") {
    *number = 0;
    *type = kDBLockFile;
  } else if (rest == "LOG" || rest == "LOG.old") {
    *number = 0;
    *type = kInfoLogFile;
  } else if (rest.starts_with("MANIFEST-")) {
    rest.remove_prefix(strlen("MANIFEST-"));
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    if (!rest.empty()) {
      return false;
    }
    *type = kDescriptorFile;
    *number = num;
  } else {
    // Avoid strtoull() to keep filename format independent of the
    // current locale
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    Slice suffix = rest;
    if (suffix == Slice(".log")) {
      *type = kLogFile;
    } else if (suffix == Slice(".bdb")) {
      *type = kTableFile;
    } else if (suffix == Slice(".dbtmp")) {
      *type = kTempFile;
    } else {
      return false;
    }
    *number = num;
  }
  return true;
}

//向sunLevel/CURRENT文件中, 写入当前的MANIFEST文件名如"MANIFEST-00001\n", 确保CURRENT刷入磁盘，关闭文件。若CURRENT已经存在,原来的被删除
Status SetCurrentFile(Env* env, const std::string& dbname,
                      uint64_t descriptor_number) {
  // Remove leading "dbname/" and add newline to manifest file name
  std::string manifest = DescriptorFileName(dbname, descriptor_number); //如sunLevel/MANIFEST-000001
  Slice contents = manifest;
  assert(contents.starts_with(dbname + "/"));
  contents.remove_prefix(dbname.size() + 1); //如 MANIFEST-000001
  std::string tmp = TempFileName(dbname, descriptor_number);// 如sunlevel/000001.dbtmp
  Status s = WriteStringToFileSync(env, contents.ToString() + "\n", tmp);//向tmp文件(将新建)中写入"MANIFEST-00001\n",确保刷入磁盘,关闭文件
  if (s.ok()) {
    s = env->RenameFile(tmp, CurrentFileName(dbname));//将temp重命名为sunLevel/CURRENT(若已经存在,原来的将被删除),这么做以防止删掉了原来的然后再失败使得不可逆
  }
  if (!s.ok()) {
    env->DeleteFile(tmp);
  }
  return s;
}

}  // namespace branchdb
