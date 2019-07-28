#include "file_in_RAM.h"
#include <assert.h>

namespace branchdb {

inline bool aligned_p(void *p, int pagesize) {
  return 0 == ((long)p & (pagesize-1));
}
inline int64_t bytes2pages(int64_t bytes, int pagesize) {
  return (bytes+pagesize-1) / pagesize;
}
inline bool is_mincore_page_resident(unsigned char p) {
  return p & 0x1;
}

//输入: 对于打开的普通文件的文件描述符fd, 对从头开始offset的长度为len的部分的页是否缓存在内存中进行统计; 
//后面三个变量既为输入也为输出;
//输出:pages_total为原值加上本次判断的总的页的数目, total_pages_in_core为原值加上本次判断为"缓存在内存的页的数目" total_pages_not_in_core为原值加上本次判断为" 没有缓存在内存的页的数目"
void file_in_RAM(int fd, int offset, int len, int pagesize, int & pages_total, int& total_pages_in_core, int& total_pages_not_in_core) {
  offset = (offset / pagesize) * pagesize;  // offset must be multiple of pagesize
  void* mem = mmap(NULL, len, PROT_READ, MAP_SHARED, fd, offset); // mem也是page size对齐的
  if(mem == MAP_FAILED || !aligned_p(mem, pagesize) ) {
    assert(0); //为了不用goto语句，这里不处理发生错误后的资源不回收(下同)
  }
  int pages = bytes2pages(len, pagesize);
  pages_total += pages;  //输出之一
  unsigned char *mincore_array = new unsigned char[pages_total];
  if (mincore_array == NULL) { assert(0); }

  if (mincore(mem, len, mincore_array)) {
    assert(0);
  }
  for (int i = 0; i < pages; i++) {
    if (is_mincore_page_resident(mincore_array[i])) {
      total_pages_in_core++;
    }
    else {
      total_pages_not_in_core++;
    }
  }

  delete [] mincore_array; //回收资源
  if (mem) {
    if(munmap(mem, len)) {
      assert(0);
    }
  }
}

} //end namespace
