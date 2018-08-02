/*
 * zeroskip-file.c
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 */

#include <stdio.h>

#include <libzeroskip/log.h>
#include <libzeroskip/zeroskip.h>
#include "zeroskip-priv.h"

#include <zlib.h>

/* static buffers for preparing records, this saves a lot malloc()'s */
#define SCRATCHBUFSIZ 65536
static unsigned char skeybuf[SCRATCHBUFSIZ];
static unsigned char svalbuf[SCRATCHBUFSIZ];
/**
 * Private functions
 */
/* Caller should free buf
 */
static int zs_prepare_key_buf(const unsigned char *key, size_t keylen,
                              unsigned char **buf, size_t *buflen,
                              int *alloced)
{
        int ret = ZS_OK;
        unsigned char *kbuf;
        size_t kbuflen, finalkeylen, pos = 0;
        enum record_t type;

        kbuflen = ZS_KEY_BASE_REC_SIZE;
        type = REC_TYPE_KEY;

        if (keylen > MAX_SHORT_KEY_LEN)
                type |= REC_TYPE_LONG;

        /* Minimum buf size */
        finalkeylen = roundup64bits(keylen);
        kbuflen += finalkeylen;

        if (kbuflen <= SCRATCHBUFSIZ) {
                memset(skeybuf, 0, SCRATCHBUFSIZ);
                kbuf = skeybuf;
                *alloced = 0;
        } else {
                kbuf = xcalloc(1, kbuflen);
                *alloced = 1;
        }

        if (type == REC_TYPE_KEY) {
                /* If it is a short key, the first 3 fields make up 64 bits */
                uint64_t val = 0;

                val = ((uint64_t)type << 56);       /* Type */
                val |= ((uint64_t)keylen << 40);    /* Key length */
                val |= ((uint64_t)kbuflen);         /* Val offset */
                write_be64(kbuf + pos, val);
                pos += sizeof(uint64_t);
                write_be64(kbuf + pos, 0ULL);     /* Extended length */
                pos += sizeof(uint64_t);
                write_be64(kbuf + pos, 0ULL);     /* Extended Value offset */
                pos += sizeof(uint64_t);

        } else {
                /* A long key has the type followed by 56 bits of nothing */
                uint64_t val = 0;
                val = ((uint64_t)type << 56);       /* Type */
                write_be64(kbuf + pos, val);
                pos += sizeof(uint64_t);
                write_be64(kbuf + pos, keylen);     /* Extended length */
                pos += sizeof(uint64_t);
                write_be64(kbuf + pos, kbuflen);    /* Extended Value offset */
                pos += sizeof(uint64_t);
        }

        /* the key */
        memcpy(kbuf + pos, key, keylen);
        pos += keylen;

        *buflen = kbuflen;
        *buf = kbuf;

        return ret;
}

/* Caller should free buf
 */
static int zs_prepare_val_buf(const unsigned char *val, size_t vallen,
                              unsigned char **buf, size_t *buflen,
                              int *alloced)
{
        int ret = ZS_OK;
        unsigned char *vbuf;
        size_t vbuflen, finalvallen, pos = 0;
        enum record_t type;

        vbuflen = ZS_VAL_BASE_REC_SIZE;
        type = REC_TYPE_VALUE;

        if (vallen > MAX_SHORT_VAL_LEN)
                type |= REC_TYPE_LONG;

        /* Minimum buf size */
        finalvallen = roundup64bits(vallen);

        vbuflen += finalvallen;

        if (vbuflen <= SCRATCHBUFSIZ) {
                memset(svalbuf, 0, SCRATCHBUFSIZ);
                vbuf = svalbuf;
                *alloced = 0;
        } else {
                vbuf = xcalloc(1, vbuflen);
                *alloced = 1;
        }

        if (type == REC_TYPE_VALUE) {
                /* The first 3 fields in a short key make up 64 bits */
                uint64_t value = 0;

                value = ((uint64_t)type << 56);     /* Type */
                value |= ((uint64_t)vallen << 32);  /* Val length */
                write_be64(vbuf + pos, value);
                pos += sizeof(uint64_t);
                write_be64(vbuf + pos, 0ULL);     /* Extended length */
                pos += sizeof(uint64_t);
        } else {
                /* A long val has the type followed by 56 bits of nothing */
                uint64_t value;
                value = ((uint64_t)type << 56);   /* Type */
                write_be64(vbuf + pos, value);
                pos += sizeof(uint64_t);
                write_be64(vbuf + pos, vallen); /* Extended length */
                pos += sizeof(uint64_t);
        }

        /* the value */
        memcpy(vbuf + pos, val, vallen);

        *buflen = vbuflen;
        *buf = vbuf;

        return ret;
}

/* Caller should free buf
 */
static int zs_prepare_delete_key_buf(const unsigned char *key, size_t keylen,
                                     unsigned char **buf, size_t *buflen,
                                     int *alloced)
{
        int ret = ZS_OK;
        unsigned char *kbuf;
        size_t kbuflen, finalkeylen, pos = 0;
        uint64_t val;
        enum record_t type = REC_TYPE_DELETED;

        kbuflen = ZS_KEY_BASE_REC_SIZE;

        /* Minimum buf size */
        finalkeylen = roundup64bits(keylen);
        kbuflen += finalkeylen;

        if (kbuflen <= SCRATCHBUFSIZ) {
                memset(skeybuf, 0, SCRATCHBUFSIZ);
                kbuf = skeybuf;
                *alloced = 0;
        } else {
                kbuf = xcalloc(1, kbuflen);
                *alloced = 1;
        }

        if (keylen <= MAX_SHORT_KEY_LEN) {
                val = ((uint64_t)type << 56);     /* Type */
                val |= ((uint64_t)keylen << 40);  /* Key length */
                write_be64(kbuf + pos, val);
                pos += sizeof(uint64_t);
                write_be64(kbuf + pos, 0ULL);     /* Extended length */
                pos += sizeof(uint64_t);
                write_be64(kbuf + pos, 0ULL);     /* Extended Value offset */
                pos += sizeof(uint64_t);
        } else {
                type = REC_TYPE_LONG_DELETED;
                val = ((uint64_t)type << 56);
                pos += sizeof(uint64_t);
                write_be64(kbuf + pos, keylen);     /* Extended length */
                pos += sizeof(uint64_t);
                write_be64(kbuf + pos, 0ULL);       /* Extended Value offset */
                pos += sizeof(uint64_t);
        }

        /* the key */
        memcpy(kbuf + pos, key, keylen);
        pos += keylen;

        *buflen = kbuflen;
        *buf = kbuf;

        return ret;
}

/**
 * Public functions
 */
int zs_file_write_keyval_record(struct zsdb_file *f,
                                const unsigned char *key, size_t keylen,
                                const unsigned char *val, size_t vallen)
{
        int ret = ZS_OK;
        size_t keybuflen, valbuflen;
        unsigned char *keybuf, *valbuf;
        size_t mfsize = 0, nbytes;
        int kalloced = 0, valloced = 0;

        if (!f->is_open)
                return ZS_NOT_OPEN;

        ret = zs_prepare_key_buf(key, keylen, &keybuf, &keybuflen, &kalloced);
        if (ret != ZS_OK) {
                return ZS_IOERROR;
        }

        ret = zs_prepare_val_buf(val, vallen, &valbuf, &valbuflen, &valloced);
        if (ret != ZS_OK) {
                return ZS_IOERROR;
        }

        /* Get the current mappedfile size */
        ret = mappedfile_size(&f->mf, &mfsize);
        if (ret) {
                zslog(LOGDEBUG, "Could not get mappedfile size: %s\n", f->fname.buf);
                goto done;
        }

        /* write key buffer */
        ret = mappedfile_write(&f->mf, (void *)keybuf,
                               keybuflen, &nbytes);
        if (ret) {
                zslog(LOGDEBUG, "Error writing key\n");
                ret = ZS_IOERROR;
                goto done;
        }

        /* assert(nbytes == keybuflen); */

        /* write value buffer */
        ret = mappedfile_write(&f->mf, (void *)valbuf,
                               valbuflen, &nbytes);
        if (ret) {
                zslog(LOGDEBUG, "Error writing key\n");
                ret = ZS_IOERROR;
                goto done;
        }

        /* assert(nbytes == valbuflen); */

        /* If we failed writing the value buffer, then restore the db file to
         * the original size we had before updating */
        if (ret != ZS_OK) {
                mappedfile_truncate(&f->mf, mfsize);
        }

        /* Flush the change to disk */
        ret = mappedfile_flush(&f->mf);
        if (ret) {
                zslog(LOGDEBUG, "Error flushing data to disk.\n");
                ret = ZS_IOERROR;
                goto done;
        }

done:
        if (kalloced) xfree(keybuf);
        if (valloced) xfree(valbuf);

        return ret;
}

/* zs_file_write_commit_record()
 * Writes a commit record to a file. If `final`, then this is final commit
 * in a packed file
 */
int zs_file_write_commit_record(struct zsdb_file *f, int final)
{
        int ret = ZS_OK;
        size_t buflen, nbytes, pos = 0;
        unsigned char buf[24], *ptr;
        uint32_t crc;

        if (!f->is_open)
                return ZS_INTERNAL;

        memset(&buf, 0, sizeof(buf));
        ptr = buf;

        if (f->mf->crc32_data_len > MAX_SHORT_VAL_LEN) {
                /* Long commit */
                uint32_t lccrc;
                uint64_t val = 0;
                struct zs_long_commit lc;

                lc.type1 = final ? REC_TYPE_LONG_FINAL : REC_TYPE_LONG_COMMIT;
                lc.length = f->mf->crc32_data_len;
                lc.type2 = REC_TYPE_2ND_HALF_COMMIT;

                /* Compute CRC32 */
                crc = crc32_end(&f->mf);

                lccrc = crc32(0L, Z_NULL, 0);

                /* Create long commit */
                val = ((uint64_t)lc.type1 << 56); /* type 1 */
                write_be64(ptr + pos, val);
                pos += sizeof(uint64_t);
                #if ZLIB_VERNUM == 0x12b0
                lccrc = crc32_z(lccrc, (void *)&val, sizeof(uint64_t));
                #else
                lccrc = crc32(lccrc, (void *)&val, sizeof(uint64_t));
                #endif

                val = lc.length;
                write_be64(ptr + pos, val); /* length */
                pos += sizeof(uint64_t);
                #if ZLIB_VERNUM == 0x12b0
                lccrc = crc32_z(lccrc, (void *)&val, sizeof(uint64_t));
                #else
                lccrc = crc32(lccrc, (void *)&val, sizeof(uint64_t));
                #endif

                val = 0;
                val = ((uint64_t)lc.type2 << 56); /* type 2 */

                /* The final CRC  */
                #if ZLIB_VERNUM == 0x12b0
                lccrc = crc32_z(lccrc, (void *)&val, sizeof(uint64_t));
                #else
                lccrc = crc32(lccrc, (void *)&val, sizeof(uint64_t));
                #endif
                lc.crc32 = crc32_combine(crc, lccrc, 3 * sizeof(uint64_t));

                val |= (uint64_t)lc.crc32;        /* crc */

                write_be64(ptr + pos, val);
                pos += sizeof(uint64_t);

                buflen = ZS_LONG_COMMIT_REC_SIZE;
        } else {
                /* Short commit */
                uint64_t val = 0;
                uint32_t sccrc;
                struct zs_short_commit sc;

                sc.type = final ? REC_TYPE_FINAL : REC_TYPE_COMMIT;
                sc.length = f->mf->crc32_data_len;

                /* Compute CRC32 */
                crc = crc32_end(&f->mf);
                sccrc = crc32(0L, Z_NULL, 0);

                val = ((uint64_t)sc.type << 56);               /* type */
                val |= ((uint64_t)sc.length << 32);            /* length */
                /* The final CRC  */
                #if ZLIB_VERNUM == 0x12b0
                sccrc = crc32_z(sccrc, (void *)&val, sizeof(uint64_t));
                #else
                sccrc = crc32(sccrc, (void *)&val, sizeof(uint64_t));
                #endif
                sc.crc32 = crc32_combine(crc, sccrc, sizeof(uint64_t));

                val |= (uint64_t)sc.crc32;                     /* crc */

                write_be64(ptr + pos, val);
                pos += sizeof(uint64_t);

                buflen = ZS_SHORT_COMMIT_REC_SIZE;
        }

        ret = mappedfile_write(&f->mf, (void *)buf,
                               buflen, &nbytes);
        if (ret) {
                zslog(LOGDEBUG, "Error writing commit record.\n");
                ret = ZS_IOERROR;
                goto done;
        }

        /* assert(nbytes == buflen); */

        /* Flush the change to disk */
        ret = mappedfile_flush(&f->mf);
        if (ret) {
                zslog(LOGDEBUG, "Error flushing commit record to disk.\n");
                ret = ZS_IOERROR;
                goto done;
        }

done:
        return ret;
}

int zs_file_write_delete_record(struct zsdb_file *f,
                                const unsigned char *key, size_t keylen)
{
        int ret = ZS_OK;
        unsigned char *dbuf;
        size_t dbuflen, mfsize = 0, nbytes;
        int alloced = 0;


        if (!f->is_open)
                return ZS_NOT_OPEN;

        ret = zs_prepare_delete_key_buf(key, keylen, &dbuf, &dbuflen, &alloced);
        if (ret != ZS_OK) {
                return ZS_IOERROR;
        }

        /* Get the current mappedfile size */
        ret = mappedfile_size(&f->mf, &mfsize);
        if (ret) {
                zslog(LOGDEBUG, "delete: Could not get mappedfile size\n");
                goto done;
        }

        /* write delete buffer */
        ret = mappedfile_write(&f->mf, (void *)dbuf,
                               dbuflen, &nbytes);
        if (ret) {
                zslog(LOGDEBUG, "Error writing delete key\n");
                ret = ZS_IOERROR;
                goto done;
        }

        /* assert(nbytes == keybuflen); */

        /* If we failed writing the delete buffer, then restore the db file to
         * the original size we had before updating */
        if (ret != ZS_OK) {
                mappedfile_truncate(&f->mf, mfsize);
        }

        /* Flush the change to disk */
        ret = mappedfile_flush(&f->mf);
        if (ret) {
                zslog(LOGDEBUG, "delete: Error flushing data to disk.\n");
                ret = ZS_IOERROR;
                goto done;
        }

done:
        if (alloced)
                xfree(dbuf);
        return ret;
}

/* zs_file_update_stat():
 * Fetch and update the `struct stat` information for a file pointed to by f.
 *
 * Returns ZS_OK when the stat() command worked, ZS_ERROR otherwise.
 */
int zs_file_update_stat(struct zsdb_file *f)
{
        if (!f->is_open)
                return ZS_INTERNAL;

        memset(&f->st, 0, sizeof(struct stat));

        return mappedfile_stat(&f->mf, &f->st) == 0 ? ZS_OK : ZS_ERROR;
}

/* zs_file_check_stat():
 * Given a file `f`, check if the stats for f have changed.
 *
 * Returns 0 if nothing has changed, and 1 otherwise.
 */
int zs_file_check_stat(struct zsdb_file *f)
{
        struct stat st;
        int status = 0;

        if (!f->is_open)
                return ZS_INTERNAL;

        if (mappedfile_stat(&f->mf, &st) != 0) {
                zslog(LOGWARNING, "Cannot stat %s.\n", f->fname.buf);
                return ZS_ERROR;
        }

        if (st.st_ino != f->st.st_ino)
                status |= ZSDB_FILE_INO_CHANGED;
        if (st.st_mode != f->st.st_mode)
                status |= ZSDB_FILE_MODE_CHANGED;
        if ((st.st_uid |= f->st.st_uid))
                status |= ZSDB_FILE_UID_CHANGED;
        if ((st.st_gid |= f->st.st_gid))
                status |= ZSDB_FILE_GID_CHANGED;
        if ((st.st_size |= f->st.st_size))
                status |= ZSDB_FILE_SIZE_CHANGED;
        if ((st.st_mtim.tv_nsec |= f->st.st_mtim.tv_nsec))
                status |= ZSDB_FILE_MTIM_CHANGED;
        if ((st.st_ctim.tv_nsec |= f->st.st_ctim.tv_nsec))
                status |= ZSDB_FILE_CTIM_CHANGED;

        return status;
}
