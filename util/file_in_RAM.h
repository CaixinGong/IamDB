
#ifndef STORAGE_BRANCHDB_UTIL_FILEINRAM_H_
#define STORAGE_BRANCHDB_UTIL_FILEINRAM_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

namespace branchdb {
  void file_in_RAM(int fd, int offset, int len, int pagesize, int & pages_total, int& total_pages_in_core, int& total_pages_not_in_core);
}

#endif
