/*
 * mfile.h
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#ifndef _MFILE_H_
#define _MFILE_H_

#include <stdio.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/uio.h>

#include <libzeroskip/log.h>

CPP_GUARD_START

struct mfile {
        char *filename;
        int fd;
        unsigned char *ptr;
        unsigned int is_dirty:1;
        unsigned int compute_crc;
        uint32_t crc32;
        size_t crc32_begin_offset;
        size_t crc32_data_len;
        size_t size;
        size_t offset;
        uint32_t flags;         /* flags passed into the mfile api */
        int mflags;             /* flags parsed into what mmap() understands */
};

enum {
        MFILE_RD     = 0x00000001,
        MFILE_WR     = 0x00000002,
        MFILE_RW     = (MFILE_RD | MFILE_WR),
        MFILE_CREATE = 0x00000010,
        MFILE_WR_CR  = (MFILE_WR | MFILE_CREATE),
        MFILE_RW_CR  = (MFILE_RW | MFILE_CREATE),
        MFILE_EXCL   = 0x00000040,
};

extern int mfile_open(const char *fname, uint32_t flags,
                           struct mfile **mfp);
#if 0                           /* Will eventually split the open() function */
extern int mfile_map(struct mfile **mfp);
#endif
extern int mfile_close(struct mfile **mfp);
extern int mfile_read(struct mfile **mfp, void *obuf,
                           size_t obufsize, size_t *nbytes);
extern int mfile_write(struct mfile **mfp, void *ibuf,
                            size_t ibufsize, size_t *nbytes);
extern int mfile_write_iov(struct mfile **mfp,
                                const struct iovec *iov,
                                unsigned int iov_cnt,
                                size_t *nbytes);
extern int mfile_size(struct mfile **mfp, size_t *psize);
extern int mfile_stat(struct mfile **mfp, struct stat *stbuf);
extern int mfile_truncate(struct mfile **mfp, size_t len);
extern int mfile_flush(struct mfile **mfp);
extern int mfile_seek(struct mfile **mfp, size_t offset,
                           size_t *newoffset);

extern void crc32_begin(struct mfile **mfp);
extern uint32_t crc32_end(struct mfile **mfp);

CPP_GUARD_END

#endif  /* _MFILE_H_ */
