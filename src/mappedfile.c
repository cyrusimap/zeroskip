/*
 * mappedfile.c
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#define _XOPEN_SOURCE 500       /* For ftruncate() see `man ftruncate` */

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <unistd.h>
#include <zlib.h>

#include "mappedfile.h"
#include <libzeroskip/util.h>

static struct mappedfile mf_init = {NULL, -1, MAP_FAILED, 0, 0, 0, 0, 0, 0, 0, 0, 0};

#define OPEN_MODE 0644

/*
  mappedfile_open():

  * Return:
    - On Success: returns 0
    - On Failure: returns non 0
 */
int mappedfile_open(const char *fname, uint32_t flags, struct mappedfile **mfp)
{
        struct mappedfile *mf;
        int mflags, oflags;
        struct stat st;
        int ret = 0;

        if (!fname) {
                fprintf(stderr, "Need a valid filename\n");
                return -1;
        }

        mf = xcalloc(1, sizeof(struct mappedfile));

        mf->filename = xstrdup(fname);
        mf->flags = flags;
        mf->fd = -1;

        if (mf->ptr != MAP_FAILED) {
                if (mf->ptr)
                        munmap(mf->ptr, mf->size);
                mf->ptr = MAP_FAILED;
        }

        /* Flags */
        if (flags & MAPPEDFILE_RW) {
                mflags = PROT_READ | PROT_WRITE;
                oflags = O_RDWR;
        } else if (flags & MAPPEDFILE_WR) {
                mflags = PROT_READ | PROT_WRITE;
                oflags = O_WRONLY;
        } else if (flags & MAPPEDFILE_RD) {
                mflags = PROT_READ;
                oflags = O_RDONLY;
        } else {                /* defaults to RDONLY */
                mflags = PROT_READ;
                oflags = O_RDONLY;
        }

        if (flags & MAPPEDFILE_CREATE)
                oflags |= O_CREAT;

        if (flags & MAPPEDFILE_EXCL)
                oflags |= O_EXCL;

        mf->fd = open(fname, oflags, OPEN_MODE);
        if (mf->fd < 0) {
                perror("mappedfile_open:open");
                return errno;
        }

        if (fstat(mf->fd, &st) != 0) {
                int err = errno;
                close(mf->fd);
                perror("mappedfile_open:fstat");
                return err;
        }

        mf->size = st.st_size;
        if (mf->size) {
                mf->ptr = mmap(0, mf->size, mflags, MAP_SHARED, mf->fd, 0);
                if (mf->ptr == MAP_FAILED) {
                        int err = errno;
                        close(mf->fd);
                        return err;
                }
        } else
                mf->ptr = NULL;

        mf->mflags = mflags;

        *mfp = mf;

        return ret;
}

/*
  XXX: At a later point in time, mappedfile_open() will be split into 2
  operations. The first one would be to open() the file and get a valid fd.
  And then the mappedfile_map() to actually map it in memory.
 */
#if 0
/*
 * mappedfile_map()
 *
 */
int mappedfile_map(struct mappedfile **mfp)
{
        if (mfp && *mfp) {
                struct mappedfile *mf = *mfp;

                if (mf == &mf_init)
                        return 0;

                if (mf->size) {
                        mf->ptr = mmap(0, mf->size, mf->mflags,
                                       MAP_SHARED, mf->fd, 0);
                        if (mf->ptr == MAP_FAILED) {
                                int err = errno;
                                close(mf->fd);
                                return err;
                        }
                } else
                        mf->ptr = NULL;
        }

        return 0;
}
#endif

/*
 * mappedfile_close()
 *
 */
int mappedfile_close(struct mappedfile **mfp)
{
        if (mfp && *mfp) {
                struct mappedfile *mf = *mfp;

                if (mf == &mf_init)
                        return 0;

                xfree(mf->filename);

                if (mf->ptr != MAP_FAILED && mf->ptr) {
                        munmap(mf->ptr, mf->size);
                        mf->ptr = MAP_FAILED;
                }

                if (mf->fd) {
                        close(mf->fd);
                        mf->fd = -1;
                }

                xfree(mf);
                *mfp = &mf_init;
        }

        return 0;
}

/*
 * mapfile_read():
 *
 *        mfp    - a pointer to a struct mappedfile object
 *        obuf   - buffer to read into
 *        osize  - bize of the buffer being read into
 *        nbytes - total number of bytes read
 *
 * Return:
 *   Success : 0
 *   Failre  : non zero
 */
int mappedfile_read(struct mappedfile **mfp, void *obuf,
                    size_t obufsize, size_t *nbytes)
{
        struct mappedfile *mf = *mfp;
        size_t n = 0;

        if (!mf)
            return EINVAL;

        if (mf == &mf_init || mf->ptr == MAP_FAILED)
                return EINVAL;

        if (mf->offset < mf->size) {
                n = ((mf->offset + obufsize) > mf->size) ?
                        mf->size - mf->offset : obufsize;

                memcpy(obuf, mf->ptr + mf->offset, n);
                mf->offset += n;
        }

        if (nbytes)
                *nbytes = n;

        return 0;
}

/*
 * mapfile_write():
 *
 *        mfp    - a pointer to a struct mappedfile object
 *        ibuf   - buffer to write from
 *        ibufsize  - size of the buffer written
 *        nbytes - total number of bytes written
 *
 * Return:
 *   Success : 0
 *   Failre  : non zero
 */
int mappedfile_write(struct mappedfile **mfp, void *ibuf, size_t ibufsize,
                     size_t *nbytes)
{
        struct mappedfile *mf = *mfp;

        if (!mf)
            return EINVAL;

        if (mf == &mf_init || mf->ptr == MAP_FAILED)
                return EINVAL;

        if (!(mf->flags & MAPPEDFILE_WR)    ||
            !(mf->flags & MAPPEDFILE_WR_CR) ||
            !(mf->flags & MAPPEDFILE_RW)    ||
            !(mf->flags & MAPPEDFILE_RW_CR))
                return EACCES;

        if (mf->size < (mf->offset + ibufsize)) {
                /* If the input buffer's size is bigger, we overwrite. */
                if (mf->ptr && munmap(mf->ptr, mf->size) != 0) {
                        int err = errno;
                        mf->ptr = MAP_FAILED;
                        close(mf->fd);
                        return err;
                }

                if (ftruncate(mf->fd, mf->offset + ibufsize) != 0)
                        return errno;

                mf->ptr = mmap(0, mf->offset + ibufsize, mf->flags,
                               MAP_SHARED, mf->fd, 0);
                if (mf->ptr == MAP_FAILED) {
                        int err = errno;
                        close(mf->fd);
                        return err;
                }

                mf->size = mf->offset + ibufsize;
        }

        if (ibufsize) {
                memcpy(mf->ptr + mf->offset, ibuf, ibufsize);
                mf->offset += ibufsize;
        }

        if (nbytes)
                *nbytes = ibufsize;

        /* compute CRC32 */
        if (mf->compute_crc) {
                mf->crc32_data_len += ibufsize;
        }

        return 0;
}

/*
 * mappedfile_write_iov():
 *
 * Return:
 *   Success : 0
 *   Failre  : non zero
 */
int mappedfile_write_iov(struct mappedfile **mfp, const struct iovec *iov,
                         unsigned int iov_cnt, size_t *nbytes)
{
        struct mappedfile *mf = *mfp;
        unsigned int i;
        size_t total_bytes = 0;

        if (!mf)
            return EINVAL;

        if (mf == &mf_init || mf->ptr == MAP_FAILED)
                return EINVAL;

        if (!(mf->flags & MAPPEDFILE_WR)    ||
            !(mf->flags & MAPPEDFILE_WR_CR) ||
            !(mf->flags & MAPPEDFILE_RW)    ||
            !(mf->flags & MAPPEDFILE_RW_CR))
                return EACCES;

        for (i = 0; i < iov_cnt; i++) {
                total_bytes += iov[i].iov_len;
        }

        if (mf->size < (mf->offset + total_bytes)) {
                /* If the input buffer's size is bigger, we overwrite. */
                if (mf->ptr && munmap(mf->ptr, mf->size) != 0) {
                        int err = errno;
                        mf->ptr = MAP_FAILED;
                        close(mf->fd);
                        return err;
                }

                if (ftruncate(mf->fd, mf->offset + total_bytes) != 0)
                        return 0;

                mf->ptr = mmap(0, mf->offset + total_bytes, mf->flags,
                               MAP_SHARED, mf->fd, 0);
                if (mf->ptr == MAP_FAILED) {
                        int err = errno;
                        close(mf->fd);
                        return err;
                }

                mf->size = mf->offset + total_bytes;
        }

        if (total_bytes) {
                for (i = 0; i < iov_cnt; i++) {
                        memcpy(mf->ptr + mf->offset, iov[i].iov_base,
                               iov[i].iov_len);
                        mf->offset += iov[i].iov_len;
                }
        }

        if (nbytes)
                *nbytes = total_bytes;

        return 0;
}

/*
  mappedfile_size():

  * Return:
    - On Success: returns 0
    - On Failure: returns non 0
 */
int mappedfile_size(struct mappedfile **mfp, size_t *psize)
{
        struct mappedfile *mf = *mfp;
        struct stat stbuf;
        int err = 0;

        memset(&stbuf, 0, sizeof(struct stat));

        if (!mf)
            return EINVAL;

        if (mf == &mf_init || mf->ptr == MAP_FAILED)
                return EINVAL;

        if (mf->ptr && (mf->flags & PROT_WRITE))
                msync(mf->ptr, mf->size, MS_SYNC);

        if (fstat(mf->fd, &stbuf) != 0)
                return errno;

        if (mf->size != (size_t) stbuf.st_size) {
                if (mf->ptr)
                        err = munmap(mf->ptr, mf->size);

                if (err != 0)
                        err = errno;
                else {
                        mf->size = stbuf.st_size;
                        if (mf->size) {
                                mf->ptr = mmap(0, mf->size, mf->flags,
                                               MAP_SHARED, mf->fd, 0);
                                if (mf->ptr == MAP_FAILED)
                                        err = errno;
                        } else
                                mf->ptr = NULL;
                }
        }

        if (err != 0) {
                mf->ptr = MAP_FAILED;
                close(mf->fd);
                mf->fd = -1;
        } else {
                if (psize)
                        *psize = stbuf.st_size;
        }

        return err;
}

/*
  mappedfile_stat():

  * Return:
    - On Success: returns 0
    - On Failure: returns non 0
 */
int mappedfile_stat(struct mappedfile **mfp, struct stat *stbuf)
{
        struct mappedfile *mf = *mfp;

        if (!mf)
            return EINVAL;

        if (mf == &mf_init || mf->ptr == MAP_FAILED)
                return EINVAL;

        if (mf->ptr && (mf->flags & PROT_WRITE))
                msync(mf->ptr, mf->size, MS_SYNC);

        if (fstat(mf->fd, stbuf) != 0)
                return errno;

        return 0;
}

/*
  mappedfile_truncate()

  * Return:
    - On Success: returns 0
    - On Failure: returns non 0
 */
int mappedfile_truncate(struct mappedfile **mfp, size_t len)
{
        struct mappedfile *mf = *mfp;
        int err = 0;

        if (!mf)
            return EINVAL;

        if (mf == &mf_init || mf->ptr == MAP_FAILED || mf->ptr == NULL)
                return EINVAL;

        if (munmap(mf->ptr, mf->size) != 0) {
                err = errno;
                mf->ptr = MAP_FAILED;
                close(mf->fd);
                return err;
        }

        if (ftruncate(mf->fd, len) != 0)
                return errno;

        mf->ptr = len ? mmap(0, len, mf->flags, MAP_SHARED, mf->fd, 0) : NULL;
        if (mf->ptr == MAP_FAILED) {
                err = errno;
                close(mf->fd);
                return err;
        }

        mf->size = len;

        return 0;
}

/*
  mappedfile_flush()

  * Return:
  - On Success: returns 0
  - On Failure: returns non 0
*/
int mappedfile_flush(struct mappedfile **mfp)
{
        struct mappedfile *mf = *mfp;

        if (!mf)
            return EINVAL;

        if (mf == &mf_init || mf->ptr == MAP_FAILED || mf->ptr == NULL)
                return EINVAL;

        if (mf->flags & PROT_WRITE)
                return msync(mf->ptr, mf->size, MS_SYNC);

        return 0;
}

/*
  mappedfile_seek()

  * Return:
  - On Success: returns 0
  - On Failure: returns non 0
*/
int mappedfile_seek(struct mappedfile **mfp, size_t offset, size_t *newoffset)
{
        struct mappedfile *mf = *mfp;

        if (!mf)
            return EINVAL;

        if (mf == &mf_init || mf->ptr == MAP_FAILED || mf->ptr == NULL)
                return EINVAL;

        if (offset > mf->size)
                return ESPIPE;

        mf->offset = offset;
        if (newoffset)
                *newoffset = offset;

        return 0;
}


void crc32_begin(struct mappedfile **mfp)
{
        (*mfp)->crc32 = crc32(0L, Z_NULL, 0);
        (*mfp)->compute_crc = 1;
        (*mfp)->crc32_begin_offset = (*mfp)->offset;
        (*mfp)->crc32_data_len = 0;
}

uint32_t crc32_end(struct mappedfile **mfp)
{
        if ((*mfp)->compute_crc) {
                (*mfp)->crc32_data_len = (*mfp)->offset - (*mfp)->crc32_begin_offset;
                (*mfp)->crc32 = crc32((*mfp)->crc32,
                                      ((*mfp)->ptr + (*mfp)->crc32_begin_offset),
                                      (*mfp)->crc32_data_len);
                (*mfp)->compute_crc = 0;
                (*mfp)->crc32_data_len = 0;
        }

        return (*mfp)->crc32;
}

