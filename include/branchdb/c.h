/* Copyright (c) 2011 The LevelDB Authors. All rights reserved.
  Use of this source code is governed by a BSD-style license that can be
  found in the LICENSE file. See the AUTHORS file for names of contributors.

  C bindings for branchdb.  May be useful as a stable ABI that can be
  used by programs that keep branchdb in a shared library, or for
  a JNI api.

  Does not support:
  . getters for the option types
  . custom comparators that implement key shortening
  . capturing post-write-snapshot
  . custom iter, db, env, cache implementations using just the C bindings

  Some conventions:

  (1) We expose just opaque struct pointers and functions to clients.
  This allows us to change internal representations without having to
  recompile clients.

  (2) For simplicity, there is no equivalent to the Slice type.  Instead,
  the caller has to pass the pointer and length as separate
  arguments.

  (3) Errors are represented by a null-terminated c string.  NULL
  means no error.  All operations that can raise an error are passed
  a "char** errptr" as the last argument.  One of the following must
  be true on entry:
     *errptr == NULL
     *errptr points to a malloc()ed null-terminated error message
       (On Windows, *errptr must have been malloc()-ed by this library.)
  On success, a branchdb routine leaves *errptr unchanged.
  On failure, branchdb frees the old value of *errptr and
  set *errptr to a malloc()ed error message.

  (4) Bools have the type unsigned char (0 == false; rest == true)

  (5) All of the pointer arguments must be non-NULL.
*/

#ifndef STORAGE_BRANCHDB_INCLUDE_C_H_
#define STORAGE_BRANCHDB_INCLUDE_C_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>

/* Exported types */

typedef struct branchdb_t               branchdb_t;
typedef struct branchdb_cache_t         branchdb_cache_t;
typedef struct branchdb_comparator_t    branchdb_comparator_t;
typedef struct branchdb_env_t           branchdb_env_t;
typedef struct branchdb_filelock_t      branchdb_filelock_t;
typedef struct branchdb_filterpolicy_t  branchdb_filterpolicy_t;
typedef struct branchdb_iterator_t      branchdb_iterator_t;
typedef struct branchdb_logger_t        branchdb_logger_t;
typedef struct branchdb_options_t       branchdb_options_t;
typedef struct branchdb_randomfile_t    branchdb_randomfile_t;
typedef struct branchdb_readoptions_t   branchdb_readoptions_t;
typedef struct branchdb_seqfile_t       branchdb_seqfile_t;
typedef struct branchdb_snapshot_t      branchdb_snapshot_t;
typedef struct branchdb_writablefile_t  branchdb_writablefile_t;
typedef struct branchdb_writebatch_t    branchdb_writebatch_t;
typedef struct branchdb_writeoptions_t  branchdb_writeoptions_t;

/* DB operations */

extern branchdb_t* branchdb_open(
    const branchdb_options_t* options,
    const char* name,
    char** errptr);

extern void branchdb_close(branchdb_t* db);

extern void branchdb_put(
    branchdb_t* db,
    const branchdb_writeoptions_t* options,
    const char* key, size_t keylen,
    const char* val, size_t vallen,
    char** errptr);

extern void branchdb_delete(
    branchdb_t* db,
    const branchdb_writeoptions_t* options,
    const char* key, size_t keylen,
    char** errptr);

extern void branchdb_write(
    branchdb_t* db,
    const branchdb_writeoptions_t* options,
    branchdb_writebatch_t* batch,
    char** errptr);

/* Returns NULL if not found.  A malloc()ed array otherwise.
   Stores the length of the array in *vallen. */
extern char* branchdb_get(
    branchdb_t* db,
    const branchdb_readoptions_t* options,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr);

extern branchdb_iterator_t* branchdb_create_iterator(
    branchdb_t* db,
    const branchdb_readoptions_t* options);

extern const branchdb_snapshot_t* branchdb_create_snapshot(
    branchdb_t* db);

extern void branchdb_release_snapshot(
    branchdb_t* db,
    const branchdb_snapshot_t* snapshot);

/* Returns NULL if property name is unknown.
   Else returns a pointer to a malloc()-ed null-terminated value. */
extern char* branchdb_property_value(
    branchdb_t* db,
    const char* propname);

extern void branchdb_approximate_sizes(
    branchdb_t* db,
    int num_ranges,
    const char* const* range_start_key, const size_t* range_start_key_len,
    const char* const* range_limit_key, const size_t* range_limit_key_len,
    uint64_t* sizes);

extern void branchdb_compact_range(
    branchdb_t* db,
    const char* start_key, size_t start_key_len,
    const char* limit_key, size_t limit_key_len);

/* Management operations */

extern void branchdb_destroy_db(
    const branchdb_options_t* options,
    const char* name,
    char** errptr);

extern void branchdb_repair_db(
    const branchdb_options_t* options,
    const char* name,
    char** errptr);

/* Iterator */

extern void branchdb_iter_destroy(branchdb_iterator_t*);
extern unsigned char branchdb_iter_valid(const branchdb_iterator_t*);
extern void branchdb_iter_seek_to_first(branchdb_iterator_t*);
extern void branchdb_iter_seek_to_last(branchdb_iterator_t*);
extern void branchdb_iter_seek(branchdb_iterator_t*, const char* k, size_t klen);
extern void branchdb_iter_next(branchdb_iterator_t*);
extern void branchdb_iter_prev(branchdb_iterator_t*);
extern const char* branchdb_iter_key(const branchdb_iterator_t*, size_t* klen);
extern const char* branchdb_iter_value(const branchdb_iterator_t*, size_t* vlen);
extern void branchdb_iter_get_error(const branchdb_iterator_t*, char** errptr);

/* Write batch */

extern branchdb_writebatch_t* branchdb_writebatch_create();
extern void branchdb_writebatch_destroy(branchdb_writebatch_t*);
extern void branchdb_writebatch_clear(branchdb_writebatch_t*);
extern void branchdb_writebatch_put(
    branchdb_writebatch_t*,
    const char* key, size_t klen,
    const char* val, size_t vlen);
extern void branchdb_writebatch_delete(
    branchdb_writebatch_t*,
    const char* key, size_t klen);
extern void branchdb_writebatch_iterate(
    branchdb_writebatch_t*,
    void* state,
    void (*put)(void*, const char* k, size_t klen, const char* v, size_t vlen),
    void (*deleted)(void*, const char* k, size_t klen));

/* Options */

extern branchdb_options_t* branchdb_options_create();
extern void branchdb_options_destroy(branchdb_options_t*);
extern void branchdb_options_set_comparator(
    branchdb_options_t*,
    branchdb_comparator_t*);
extern void branchdb_options_set_filter_policy(
    branchdb_options_t*,
    branchdb_filterpolicy_t*);
extern void branchdb_options_set_create_if_missing(
    branchdb_options_t*, unsigned char);
extern void branchdb_options_set_error_if_exists(
    branchdb_options_t*, unsigned char);
extern void branchdb_options_set_paranoid_checks(
    branchdb_options_t*, unsigned char);
extern void branchdb_options_set_env(branchdb_options_t*, branchdb_env_t*);
extern void branchdb_options_set_info_log(branchdb_options_t*, branchdb_logger_t*);
extern void branchdb_options_set_write_buffer_size(branchdb_options_t*, size_t);
extern void branchdb_options_set_max_open_files(branchdb_options_t*, int);
extern void branchdb_options_set_cache(branchdb_options_t*, branchdb_cache_t*);
extern void branchdb_options_set_block_size(branchdb_options_t*, size_t);
extern void branchdb_options_set_block_restart_interval(branchdb_options_t*, int);

enum {
  branchdb_no_compression = 0,
  branchdb_snappy_compression = 1
};
extern void branchdb_options_set_compression(branchdb_options_t*, int);

/* Comparator */

extern branchdb_comparator_t* branchdb_comparator_create(
    void* state,
    void (*destructor)(void*),
    int (*compare)(
        void*,
        const char* a, size_t alen,
        const char* b, size_t blen),
    const char* (*name)(void*));
extern void branchdb_comparator_destroy(branchdb_comparator_t*);

/* Filter policy */

extern branchdb_filterpolicy_t* branchdb_filterpolicy_create(
    void* state,
    void (*destructor)(void*),
    char* (*create_filter)(
        void*,
        const char* const* key_array, const size_t* key_length_array,
        int num_keys,
        size_t* filter_length),
    unsigned char (*key_may_match)(
        void*,
        const char* key, size_t length,
        const char* filter, size_t filter_length),
    const char* (*name)(void*));
extern void branchdb_filterpolicy_destroy(branchdb_filterpolicy_t*);

extern branchdb_filterpolicy_t* branchdb_filterpolicy_create_bloom(
    int bits_per_key);

/* Read options */

extern branchdb_readoptions_t* branchdb_readoptions_create();
extern void branchdb_readoptions_destroy(branchdb_readoptions_t*);
extern void branchdb_readoptions_set_verify_checksums(
    branchdb_readoptions_t*,
    unsigned char);
extern void branchdb_readoptions_set_fill_cache(
    branchdb_readoptions_t*, unsigned char);
extern void branchdb_readoptions_set_snapshot(
    branchdb_readoptions_t*,
    const branchdb_snapshot_t*);

/* Write options */

extern branchdb_writeoptions_t* branchdb_writeoptions_create();
extern void branchdb_writeoptions_destroy(branchdb_writeoptions_t*);
extern void branchdb_writeoptions_set_sync(
    branchdb_writeoptions_t*, unsigned char);

/* Cache */

extern branchdb_cache_t* branchdb_cache_create_lru(size_t capacity);
extern void branchdb_cache_destroy(branchdb_cache_t* cache);

/* Env */

extern branchdb_env_t* branchdb_create_default_env();
extern void branchdb_env_destroy(branchdb_env_t*);

/* Utility */

/* Calls free(ptr).
   REQUIRES: ptr was malloc()-ed and returned by one of the routines
   in this file.  Note that in certain cases (typically on Windows), you
   may need to call this routine instead of free(ptr) to dispose of
   malloc()-ed memory returned by this library. */
extern void branchdb_free(void* ptr);

/* Return the major version number for this release. */
extern int branchdb_major_version();

/* Return the minor version number for this release. */
extern int branchdb_minor_version();

#ifdef __cplusplus
}  /* end extern "C" */
#endif

#endif  /* STORAGE_BRANCHDB_INCLUDE_C_H_ */
