// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <deque>
#include <set>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#if defined(BRANCHDB_PLATFORM_ANDROID)
#include <sys/stat.h>
#endif
#include "branchdb/env.h"
#include "branchdb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/posix_logger.h"
#include "util/threadPool.h"
#include "db/dbformat.h"
namespace branchdb {

namespace {//未命名命名空间,只可在本文件内部使用

static Status IOError(const std::string& context, int err_number) {
  return Status::IOError(context, strerror(err_number));//strerror()标准库函数return string describing error number(errno)
}

class PosixSequentialFile: public SequentialFile {
 private:
  std::string filename_;
  FILE* file_;

 public:
  PosixSequentialFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }
  virtual ~PosixSequentialFile() { fclose(file_); }
  
  //输入参数scratch的空间要足够大,读取n bytes数据，内容存放在scratch中，由result链接引用，包括长度r,
  // 当读取内容小于n时，若到达文件末尾正常退出，否则返回错误消息
  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;                                                  
    size_t r = fread_unlocked(scratch, 1, n, file_);//#define fread_unlocked fread
    *result = Slice(scratch, r);//用了默认的=操作符重载
    if (r < n) {
      if (feof(file_)) {
        // We leave status as ok if we hit the end of the file
      } else {
        // A partial read with an error: return a non-ok status
        s = IOError(filename_, errno);
      }
    }
    return s;
  }

  virtual Status Skip(uint64_t n) {//从当前位置后移n个bytes
    if (fseek(file_, n, SEEK_CUR)) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }
};

// pread() based random-access 即读是原子的，和PosixPwritableFile类配合使用
class PosixRandomAccessFile: public RandomAccessFile {
 private:
  std::string filename_;
  int fd_;

 public:
  PosixRandomAccessFile(const std::string& fname, int fd)
      : filename_(fname), fd_(fd) { }
  virtual ~PosixRandomAccessFile() { close(fd_); }

  //从文件中的offset开始出读n个字节到scratch中，result的内容与scratch关联
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    ssize_t r = pread(fd_, scratch, n, static_cast<off_t>(offset));//从文件描述符fd_中的offset处读n个字符到scratch中,返回实际读到字节数
    *result = Slice(scratch, (r < 0) ? 0 : r);
    if (r < 0) {
      // An error: return a non-ok status
      s = IOError(filename_, errno);
    }
    return s;
  }
};

// Helper class to limit mmap file usage so that we do not end up
// running out virtual memory or running into kernel performance
// problems for very large databases.
class MmapLimiter {
 public:
  // Up to config::mmapReadFileNum(原来1000,现在默认为0) mmaps for 64-bit binaries; none for smaller pointer sizes.
  MmapLimiter() {
    SetAllowed(sizeof(void*) >= 8 ? config::mmapReadFileNum : 0); //config::mapFileNum为0，默认关闭mmap读取文件
  }

  // If another mmap slot is available, acquire it and return true.
  // Else return false.
  bool Acquire() {
    if (GetAllowed() <= 0) {  //本函数运用了"double-checked locking"技术
      return false;
    }
    MutexLock l(&mu_);  //这个锁必须加，因为虽然是原子指针，但是下面的get，set之间需要互斥
    intptr_t x = GetAllowed();
    if (x <= 0) {
      return false;
    } else {
      SetAllowed(x - 1);
      return true;
    }
  }

  // Release a slot acquired by a previous call to Acquire() that returned true.
  void Release() {
    MutexLock l(&mu_);//这个锁必须加，因为虽然是原子指针，但是下面的get，set之间需要互斥
    SetAllowed(GetAllowed() + 1);
  }

 private:
  port::Mutex mu_;
  port::AtomicPointer allowed_;

  intptr_t GetAllowed() const {
    return reinterpret_cast<intptr_t>(allowed_.Acquire_Load());
  }

  // REQUIRES: mu_ must be held
  void SetAllowed(intptr_t v) {
    allowed_.Release_Store(reinterpret_cast<void*>(v));
  }

  MmapLimiter(const MmapLimiter&);
  void operator=(const MmapLimiter&);
};

// mmap() based random-access
//通过mmap进行读取操作, 读是原子的，可和PosixPwritableFile类配合使用
class PosixMmapReadableFile: public RandomAccessFile {
 private:
  std::string filename_;
  void* mmapped_region_;
  size_t length_;
  MmapLimiter* limiter_;

 public:
  // base[0,length-1] contains the mmapped contents of the file.
  // base[0,length-1]已经mapped了整个文件的内容,其作为参数传进来,见PosixEnv::NewRandomAccessFile使用))
  PosixMmapReadableFile(const std::string& fname, void* base, size_t length,
                        MmapLimiter* limiter)
      : filename_(fname), mmapped_region_(base), length_(length),
        limiter_(limiter) {
  }

  //取消此次mmap
  virtual ~PosixMmapReadableFile() {
    munmap(mmapped_region_, length_);
    limiter_->Release();
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    if (offset + n > length_) {
      *result = Slice();
      s = IOError(filename_, EINVAL);
    } else {
      *result = Slice(reinterpret_cast<char*>(mmapped_region_) + offset, n);
    }
    return s;
  }
};

//注意这个类是基于是先在stdio.h库的内存的操作的，后面才刷入内核空间
class PosixWritableFile : public WritableFile {
 private:
  std::string filename_;
  FILE* file_;

 public:
  PosixWritableFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }

  ~PosixWritableFile() {
    if (file_ != NULL) {
      // Ignoring any potential errors
      fclose(file_);
    }
  }

  virtual Status Append(const Slice& data) {
    size_t r = fwrite_unlocked(data.data(), 1, data.size(), file_);//#define fwrite_unlocked fwrite(写二进制数据，允许'/0'字节）
    if (r != data.size()) {
      return IOError(filename_, errno);//返回一个设置了Status对象中const char* state_成员为相应错误消息的对象
    }                                  // 同一命名空间的函数(branchdb::未命名::, 故本函数和该类只可在本文件内部使用)
    return Status::OK();
  }

  virtual Status Close() {
    Status result;
    if (fclose(file_) != 0) {
      result = IOError(filename_, errno);
    }
    file_ = NULL;
    return result;
  }

  virtual Status Flush() {
    if (fflush_unlocked(file_) != 0) {//#define fflush_unlocked fflush,将缓冲区的内容冲洗至内核
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  //如果文件为MANIFEST， 则对MANIFEST所在的目录进行"sync"操作确保数据（包括数据部分，和文件的属性）写入磁盘
  Status SyncDirIfManifest() {
    const char* f = filename_.c_str(); //对于MANIFEST文件 filename_可能为，sunLevel/MANIFEST-000002
    const char* sep = strrchr(f, '/');
    //提取目录：dir， 文件名basename
    Slice basename;
    std::string dir;
    if (sep == NULL) {
      dir = ".";
      basename = f;
    } else {
      dir = std::string(f, sep - f);
      basename = sep + 1;
    }
    Status s;
    //如果文件为MANIFEST， 则对MANIFEST所在的目录进行sync操作确保数据（包括数据部分，和文件的属性）写入磁盘
    if (basename.starts_with("MANIFEST")) { //确保是MANIFEST文件
      int fd = open(dir.c_str(), O_RDONLY); //打开的是目录
      if (fd < 0) {
        s = IOError(dir, errno);
      } else { //打开成功
        if (fsync(fd) < 0) { //确保数据（包括数据部分，和文件的属性）写入磁盘
          s = IOError(dir, errno);
        }
        close(fd);
      }
    }
    return s;
  }
 
  //如果是Manifest 文件，先对所在的目录fsync, 再对Manifest文件调用fdatasync(只影响文件的数据部分，不影响文件的属性）以确保写入磁盘才返回
  //否则只对文件调用fdatasync(只影响文件的数据部分，不影响文件的属性）以确保写入磁盘才返回
  virtual Status Sync() {
    // Ensure new files referred to by the manifest are in the filesystem.
    Status s = SyncDirIfManifest();//如果文件为MANIFEST， 则对MANIFEST所在的"目录"进行sync操作确保数据（包括数据部分，和文件的属性）写入磁盘
    if (!s.ok()) {
      return s;
    }
    if (fflush_unlocked(file_) != 0 ||  //caixin: #define fflush_unlocked fflush
        fdatasync(fileno(file_)) != 0) { //确保写入磁盘才返回(只影响文件的数据部分，不影响文件的属性）
      s = Status::IOError(filename_, strerror(errno));
    }
    return s;
  }
};

//注意本类为新添加的，首先由用户内存使写小数据时速度会更快
//构造函数传入的f的打开方式不能为O_APPEND的方式
//多线程不安全的
class PosixRandomWritableFile : public RandomWritableFile {
 private:
  std::string  filename_;
  FILE* file_;
 public:
  
  PosixRandomWritableFile(const std::string& fname, FILE* f)
      : filename_(fname), file_(f) { }

  ~PosixRandomWritableFile() {
    if (file_ != NULL) {
      // Ignoring any potential errors
      fclose(file_);
    }
  }

  virtual Status AllocateSpace(uint64_t offset, uint64_t len) {
    int fd = fileno(file_);
    int r = posix_fallocate(fd, offset, len); //若错误不设置errno, 体现在返回值
    if (r != 0) {
      return IOError(filename_, r);//返回一个设置了Status对象中const char* state_成员为相应错误消息的对象
    }                                  // 同一命名空间的函数(branchdb::未命名::, 故本函数和该类只可在本文件内部使用)
    return Status::OK();
  }

  virtual Status Seek(uint64_t offset) {
    int r = fseek(file_, offset, SEEK_SET);
    if (r != 0) {
      return IOError(filename_, errno);//返回一个设置了Status对象中const char* state_成员为相应错误消息的对象
    }                                  // 同一命名空间的函数(branchdb::未命名::, 故本函数和该类只可在本文件内部使用)
    return Status::OK();
 }

  virtual Status Write(const Slice& data ) {
    size_t r = fwrite_unlocked(data.data(), 1, data.size(), file_);//#define fwrite_unlocked fwrite(写二进制数据，允许/0字节）
    if (r != data.size()) {
      return IOError(filename_, errno);//返回一个设置了Status对象中const char* state_成员为相应错误消息的对象
    }                                  // 同一命名空间的函数(branchdb::未命名::, 故本函数和该类只可在本文件内部使用)
    return Status::OK();
 }

  virtual Status Close() {
    Status result;
    if (fclose(file_) != 0) {
      result = IOError(filename_, errno);
    }
    file_ = NULL;
    return result;
 }

  virtual Status Flush() {
    if (fflush_unlocked(file_) != 0) {//#define fflush_unlocked fflush,将缓冲区的内容冲洗至内核
      return IOError(filename_, errno);
    }
    return Status::OK();
 }

  virtual Status Sync() {
    Status s;
    if (fflush_unlocked(file_) != 0 ||  //caixin: #define fflush_unlocked fflush
        fdatasync(fileno(file_)) != 0) { //确保写入磁盘才返回(只影响文件的数据部分，不影响文件的属性）
      s = IOError(filename_, errno);
    }
    return s;
 }

};

//写是原子的
class PosixPwritableFile : public PwritableFile {
 private:
  std::string  filename_;
  int fd_;
 public:
  
  PosixPwritableFile(const std::string& fname, int fd)
      : filename_(fname), fd_(fd) { }

  ~PosixPwritableFile() {
    if (fd_ != 0) {
      // Ignoring any potential errors
      close(fd_);
    }
  }

  virtual Status AllocateSpace(uint64_t offset, uint64_t len) {
    int r = posix_fallocate(fd_, offset, len); //错误不设置errno
    if (r != 0) {
      return IOError(filename_, r);//返回一个设置了Status对象中const char* state_成员为相应错误消息的对象
    }                                  // 同一命名空间的函数(branchdb::未命名::, 故本函数和该类只可在本文件内部使用)
    return Status::OK();
  }

 
 //写是原子的, 可以与其他原子的读写正常进行; 本函数不负责提供缓冲机制，所以如果想性能好需要调用pwrite写的数据量尽量大
  virtual Status Pwrite(uint64_t offset, const Slice& data ) {
    size_t r = pwrite(fd_, data.data(), data.size(), offset);//#define fwrite_unlocked fwrite(写二进制数据，允许/0字节）
    if (r != data.size()) {
      return IOError(filename_, errno);//返回一个设置了Status对象中const char* state_成员为相应错误消息的对象
    }                                  // 同一命名空间的函数(branchdb::未命名::, 故本函数和该类只可在本文件内部使用)
    return Status::OK();
 }

  virtual Status Close() {
    Status result;
    if (close(fd_) != 0) {
      result = IOError(filename_, errno);
    }
    fd_ = 0;
    return result;
 }

  virtual Status Sync() {
    Status s;
    if (fdatasync(fd_) != 0) { //确保写入磁盘才返回(只影响文件的数据部分，不影响文件的属性）
      s = Status::IOError(filename_, strerror(errno));
    }
    return s;
 }

};

//根据lock，为true：对fd整个文件加独占性写锁; 为false：对该文件解锁
//注意记录锁只对进程间有效
static int LockOrUnlock(int fd, bool lock) {
  errno = 0;
  struct flock f;
  memset(&f, 0, sizeof(f));
  f.l_type = (lock ? F_WRLCK : F_UNLCK); //独占性写锁，或解锁
  f.l_whence = SEEK_SET;
  f.l_start = 0;
  f.l_len = 0;        // Lock/unlock entire file
  return fcntl(fd, F_SETLK, &f);
}

class PosixFileLock : public FileLock {
 public:
  int fd_;
  std::string name_;
};

// Set of locked files.  We keep a separate set instead of just
// relying on fcntrl(F_SETLK)（记录锁在进程间使用，线程间无效） since fcntl(F_SETLK) does not provide
// any protection against multiple uses from the same process.
// 使用默认构造函数
//记录了需要lock的file的name(对于多线程之间)
class PosixLockTable {
 private:
  port::Mutex mu_; //用于保护locked_files_的修改
  std::set<std::string> locked_files_;
 public:
  bool Insert(const std::string& fname) {
    MutexLock l(&mu_);
    return locked_files_.insert(fname).second;
  }
  void Remove(const std::string& fname) {
    MutexLock l(&mu_);
    locked_files_.erase(fname);
  }
};

class PosixEnv : public Env {
 public:
  PosixEnv();
  virtual ~PosixEnv() {
    //内部的资源不能主动回收如创建的线程池，只有当本进程结束后才会被操作系统回收
    fprintf(stderr, "Destroying Env::Default()\n");
    abort();
  }

  //将文件fname打开，并new PosixSequentialFile对象赋值给SequentialFile返回
  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) {
    FILE* f = fopen(fname.c_str(), "r");
    if (f == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PosixSequentialFile(fname, f);
      return Status::OK();
    }
  }

  //将fname文件打开,若为64位系统且已经mmap(mmap整个文件的大小)文件的个数小于等于config::mmapReadFileNum,则使用mmap的方式构造RandomAccessFile对象返回; 否则使用malloc+pread的方式否则RandomAccessFile对象以读取整个文件
  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) {
    *result = NULL;
    Status s;
    int fd = open(fname.c_str(), O_RDONLY);
    if (fd < 0) {
      s = IOError(fname, errno);
    } else if (mmap_limit_.Acquire()) {
      uint64_t size;
      s = GetFileSize(fname, &size);
      if (s.ok()) {
        void* base = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
        if (base != MAP_FAILED) {
          *result = new PosixMmapReadableFile(fname, base, size, &mmap_limit_);
        } else {
          s = IOError(fname, errno);
        }
      }
      close(fd);
      if (!s.ok()) {
        mmap_limit_.Release();
      }
    } else {
      *result = new PosixRandomAccessFile(fname, fd);
    }
    return s;
  }

  //打开fname文件作为参数新建PosixWritableFile对象写入result(fname文件存不存在都可)
  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result,
                                 bool isTruncated = true) {
    Status s;
    FILE* f = NULL;
    if(isTruncated)
      f = fopen(fname.c_str(), "w");
    else
      f = fopen(fname.c_str(), "r+");
    if (f == NULL) {
      *result = NULL;
      s = IOError(fname, errno);
    } else {
      *result = new PosixWritableFile(fname, f);
    }
    return s;
  }

  virtual Status NewRandomWritableFile( const std::string& fname,
                                     RandomWritableFile **result) {
    Status s;
    bool isExist =  FileExists(fname);
    if(!isExist) {
      int fd = open(fname.c_str(), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP);
      if(fd == -1) {
        *result = NULL;
        s = IOError(fname, errno);
        return s;
      }
      int r = close(fd);
      if(r == -1) {
        *result = NULL;
        s = IOError(fname, errno);
        return s;
      }
    }
    FILE* f = fopen(fname.c_str(), "r+"); //读和写的方式打开的文件,不能有O_APPEND
    if (f == NULL) {
      *result = NULL;
      s = IOError(fname, errno);
    } else {
      *result = new PosixRandomWritableFile(fname, f);
    }
    return s;
  }

  //若原文件不存在则新建，若存在则打开原文件                                    
  virtual Status NewPwritableFile( const std::string& fname,
                                     PwritableFile **result) {
    Status s;
    int fd = open(fname.c_str(), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP); //如果不存在创建一个文件,如果存在则直接打开
    if(fd == -1) {
      *result = NULL;
      s = IOError(fname, errno);
      return s;
    }
    *result = new PosixPwritableFile(fname, fd); //将打开的文件描述符fd封装到一个类中
    return s;
  }
  
  //判断文件fname是否已经存在，是返回true
  virtual bool FileExists(const std::string& fname) {
    return access(fname.c_str(), F_OK) == 0;
  }

  //将dir目录下的每一个文件名push_back至*result
  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) {
    result->clear();
    DIR* d = opendir(dir.c_str());
    if (d == NULL) {
      return IOError(dir, errno);
    }
    struct dirent* entry;
    while ((entry = readdir(d)) != NULL) {
      result->push_back(entry->d_name);
    }
    closedir(d);
    return Status::OK();
  }

  //即unlink fname
  virtual Status DeleteFile(const std::string& fname) {
    Status result;
    if (unlink(fname.c_str()) != 0) {
      result = IOError(fname, errno);
    }
    return result;
  }

  //创建目录，用户:读写执行; 组: 读 执行; 其他: 读 执行
  //若存在目录创建不成功（具体看man 手册）
  virtual Status CreateDir(const std::string& name) {
    Status result;
    if (mkdir(name.c_str(), 0755) != 0) {
      result = IOError(name, errno);
    }
    return result;
  }

  virtual Status DeleteDir(const std::string& name) {
    Status result;
    if (rmdir(name.c_str()) != 0) {
      result = IOError(name, errno);
    }
    return result;
  }

  //获得文件fname的大小，存至*size
  virtual Status GetFileSize(const std::string& fname, uint64_t* size) {
    Status s;
    struct stat sbuf;
    if (stat(fname.c_str(), &sbuf) != 0) {
      *size = 0;
      s = IOError(fname, errno);
    } else {
      *size = sbuf.st_size;
    }
    return s;
  }

 //文件重命名(若target已经存在，将被删除，若src不存在，返回-1，errno设置为 ENOENT)
 //src : oldname  target : newname
  virtual Status RenameFile(const std::string& src, const std::string& target) {
    Status result;
    if (rename(src.c_str(), target.c_str()) != 0) { //系统调用(若target即new name已经存在，将被删除，若src不存在，返回-1，errno设置为 ENOENT)
      result = IOError(src, errno);
    }
    return result;
  }

  //打开fname(dbname/LOCK)成功,将fname插入this(PosixEnv*类型)->locks_(记录了加锁的文件名而已,set of locked files)中.
  //对整个文件加独占性写锁
  //新建PosixFileLock对象(只有两个成员，fd_ name_而已)设置成员写到lock返回
  virtual Status LockFile(const std::string& fname, FileLock** lock) {
    *lock = NULL;
    Status result;
    int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);//打开文件，用户可读可写，组和其他只能读
    if (fd < 0) {
      result = IOError(fname, errno);
    } else if (!locks_.Insert(fname)) {//打开成功,将fname插入this(PosixEnv*类型)->locks_(记录了加锁的文件名而已)中.插入成功:执行下一个else if
      close(fd);
      result = Status::IOError("lock " + fname, "already held by process");
    } else if (LockOrUnlock(fd, true) == -1) {//对fd整个文件加独占性写锁,加锁成功执行下面else,否则关闭该文件，且将其从locks_中移出
      result = IOError("lock " + fname, errno);
      close(fd);
      locks_.Remove(fname);
    } else {                            //新建PosixFileLock对象(只是两个成员fd, name而已),设置成员写到lock返回
      PosixFileLock* my_lock = new PosixFileLock;
      my_lock->fd_ = fd;
      my_lock->name_ = fname;
      *lock = my_lock;
    }
    return result;
  }

  //与上述函数相反
  virtual Status UnlockFile(FileLock* lock) {
    PosixFileLock* my_lock = reinterpret_cast<PosixFileLock*>(lock);
    Status result;
    if (LockOrUnlock(my_lock->fd_, false) == -1) {
      result = IOError("unlock", errno);
    }
    locks_.Remove(my_lock->name_);
    close(my_lock->fd_);
    delete my_lock;
    return result;
  }

//以下两个函数和env的接口可以删除*********************************************
  virtual void Schedule(void (*function)(void*), void* arg); //类外部定义

  virtual void StartThread(void (*function)(void* arg), void* arg); //类外部定义

  virtual Status GetTestDirectory(std::string* result) {
    const char* env = getenv("TEST_TMPDIR");
    if (env && env[0] != '\0') {
      *result = env;
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "/tmp/branchdbtest-%d", int(geteuid()));
      *result = buf;
    }
    // Directory may already exist
    CreateDir(*result);
    return Status::OK();
  }

  static uint64_t gettid() {
    pthread_t tid = pthread_self();
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    return thread_id;
  }

  //result为输出：打开fanme(dbname/LOG)文件(若不存在则创建)以创建PosixLogger对象，PosixLogger*赋值给result
  virtual Status NewLogger(const std::string& fname, Logger** result) {
    FILE* f = fopen(fname.c_str(), "w"); //O_WRONLY|O_CREAT|O_APPEND
    if (f == NULL) {
      *result = NULL;
      return IOError(fname, errno);
    } else {
      *result = new PosixLogger(f, &PosixEnv::gettid);
      return Status::OK();
    }
  }

  //返回当前的时间，从1970年1月1日00：00：00开始(以微秒为单位)
  virtual uint64_t NowMicros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  virtual void SleepForMicroseconds(int micros) {
    usleep(micros);
  }

  //将任务放进线程池的队列中(需满足在数据库关闭时不能再放), mutex_p, cond_p用于在任务完成时通过condp进行signal以唤醒等待线程(用于在数据库关闭时判断正在执行本数据库任务的线程是否已经完成)
  virtual void Enq_task(void (*function)(void*), void* arg, port::Mutex* mutexp, port::CondVar* condp) {
    threads->Enq_task(function, arg, mutexp, condp);
  }
  //移除线程池队列中所有与function(arg)相同的任务
  virtual void RemoveAllDBTask(void (*function)(void*), void* arg) {
    threads->RemoveAllDBTask(function, arg);
  }
  //后台线程是否有正在执行function(arg)任务的
  virtual bool isDoingForDB(void (*function)(void*), void * arg) {
    return threads->isDoingForDB(function, arg);
  }


 private:
  //若result !=0 输出错误
  void PthreadCall(const char* label, int result) {
    if (result != 0) {
      fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
      abort();
    }
  }

  // BGThread() is the body of the background thread
  void BGThread(); //类外部定义
  static void* BGThreadWrapper(void* arg) {
    reinterpret_cast<PosixEnv*>(arg)->BGThread();
    return NULL;
  }

  pthread_mutex_t mu_;
  pthread_cond_t bgsignal_;
  pthread_t bgthread_;
  bool started_bgthread_;

  // Entry per Schedule() call
  struct BGItem { void* arg; void (*function)(void*); };
  typedef std::deque<BGItem> BGQueue;
  BGQueue queue_;

  PosixLockTable locks_;//记录了需要lock的file的name(对于多线程之间)
  MmapLimiter mmap_limit_;

  bool createThreadsSucc;    //构造函数赋值后不再变化，多线程安全
  ThreadPool* const threads; //多线程安全

}; //PosixEnv类定义结束

//构造函数，locks_  mmap_limit_调用了默认构造函数,暂时将后台线程数设为1
PosixEnv::PosixEnv() : started_bgthread_(false), createThreadsSucc(false), threads(new branchdb::ThreadPool(config::backThreadNum, createThreadsSucc)) {
  assert(createThreadsSucc);
  PthreadCall("mutex_init", pthread_mutex_init(&mu_, NULL)); //初始化互斥量
  PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, NULL)); //初始化条件变量
}

//在DB::open函数中，此时已经持有锁，确保单进程单线程进入
//MaybeScheduleCompaction函数调用时使用的参数为 &DBImpl::BGWork, this(DBImpl *)
//新建一个线程，将以上两个参数作为一组压入queue_中让新线程从该队列中取出并执行, 父线程退出本函数
void PosixEnv::Schedule(void (*function)(void*), void* arg) {
  PthreadCall("lock", pthread_mutex_lock(&mu_)); //注意这里加的锁是PosixEnv里的成员,即另外一个互斥量，这里要加锁是为了使用条件变量bgsignal_和共享变量queue_

  // Start background thread if necessary
  if (!started_bgthread_) { //started_bgthread_ 初始为false
    started_bgthread_ = true;
    PthreadCall(
        "create thread",
        pthread_create(&bgthread_, NULL,  &PosixEnv::BGThreadWrapper, this)); //创建一个线程，线程号保存着bgthread_, 该线程执行BGThreadWrapper函数，传递的参数为this(PosixEnv*类型)
  }

  // If the queue is currently empty, the background thread may currently be
  // waiting.
  if (queue_.empty()) {
    PthreadCall("signal", pthread_cond_signal(&bgsignal_));
  }

  // Add to priority queue
  queue_.push_back(BGItem());
  queue_.back().function = function;
  queue_.back().arg = arg;

  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

//PosixEnv::Schedule函数新建的线程将执行的routine: 队列不空，获取队列头的的function,和arg执行：(*functon)(arg)
void PosixEnv::BGThread() {
  while (true) {
    // Wait until there is an item that is ready to run(即父线程已经退出Schedule函数，执行其他工作了，此时队列不空)
    PthreadCall("lock", pthread_mutex_lock(&mu_));
    while (queue_.empty()) {
      PthreadCall("wait", pthread_cond_wait(&bgsignal_, &mu_));
    }

    void (*function)(void*) = queue_.front().function;
    void* arg = queue_.front().arg;
    queue_.pop_front();

    PthreadCall("unlock", pthread_mutex_unlock(&mu_));
    (*function)(arg);
  }
}

namespace {
struct StartThreadState {
  void (*user_function)(void*);
  void* arg;
};
}
static void* StartThreadWrapper(void* arg) { //线程执行的route
  StartThreadState* state = reinterpret_cast<StartThreadState*>(arg);
  state->user_function(state->arg);
  delete state;
  return NULL;
}

void PosixEnv::StartThread(void (*function)(void* arg), void* arg) {
  pthread_t t;
  StartThreadState* state = new StartThreadState;
  state->user_function = function;
  state->arg = arg;
  PthreadCall("start thread",
              pthread_create(&t, NULL,  &StartThreadWrapper, state));
}

}  // namespace(未命名)

static pthread_once_t once = PTHREAD_ONCE_INIT; //必须是一个非本地变量(全局变量或静态变量)
static Env* default_env;
static void InitDefaultEnv() { default_env = new PosixEnv; }

Env* Env::Default() {
  pthread_once(&once, InitDefaultEnv);//多线程之间只会执行一次
  return default_env; //所以若多个线程调用Env::Default()返回的是同一个对象的指针
}

}  // namespace branchdb
