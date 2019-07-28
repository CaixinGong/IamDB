#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <assert.h>
#include <iostream>

//通过实验发现文件内部有洞的情况，整个范围的通过mincore函数判断是否在RAM上时,不能简单的得到结果，故需要分段判断

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

int main () {
  int pagesize = sysconf(_SC_PAGESIZE);

  const char* filePath = "./testfile";
  int fd = -1;
  const int offset[2] = {100, 64*1024*1024-1}; //使得最后的不一定是4KB对齐的
  const int len[2] = { 2*1024*1024+123, 16*1024};
  if(access(filePath, F_OK) != 0) { //文件不存在
    int open_flags = O_RDWR|O_CREAT;
    fd = open(filePath, open_flags, S_IRUSR|S_IWUSR);
    if(fd == -1) {
  	  std::cout << "err 1" << std::endl;
  	  return 0;
    }
  	for(int i = 0; i < 2; ++i) {
        char tmp[len[i]];
        for(int k = 0; k < len[i]; ++k) {
          tmp[k] = 'a'+i;
        }
        lseek(fd, offset[i], SEEK_SET);
        write(fd, tmp, len[i]); 
    }
  } else { //文件已经存在,且假设是上次运行时上面创建好的
    int open_flags = O_RDONLY | O_NOATIME;
    fd = open(filePath, open_flags, 0);
    if(fd == -1) {
  	  std::cout << "err 2" << std::endl;
  	  return 0;
    }
  }

  int pages_total = 0, total_pages_in_core = 0, total_pages_not_in_core = 0; //..........需要赋初始值，因为函数内部直接基于原值进行操作...........
  file_in_RAM(fd, offset[0], len[0], pagesize, pages_total, total_pages_in_core, total_pages_not_in_core);
  file_in_RAM(fd, offset[1], len[1], pagesize, pages_total, total_pages_in_core, total_pages_not_in_core);
  std::cout <<"pages_total: " << pages_total <<  ", total_pages_in_core: " << total_pages_in_core << ", total_pages_not_in_core: " << total_pages_not_in_core << std::endl;

  if (fd != -1) {
    close(fd);
  }
  return 0;
}

