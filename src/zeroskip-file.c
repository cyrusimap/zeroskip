/*
 * zeroskip-file.c
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 */

#include <stdio.h>

#include "log.h"
#include "zeroskip.h"
#include "zeroskip-priv.h"

#include <zlib.h>
/**
 * Private functions
 */


/**
 * Public functions
 */
int zs_file_write_keyval_record(struct zsdb_file *f,
                                unsigned char *key, size_t keylen,
                                unsigned char *val, size_t vallen)
{
        int ret = ZS_OK;
        size_t keybuflen, valbuflen;
        unsigned char *keybuf, *valbuf;
        size_t mfsize, nbytes;

        if (!f->is_open)
                return ZS_NOT_OPEN;

        ret = zs_prepare_key_buf(key, keylen, &keybuf, &keybuflen);
        if (ret != ZS_OK) {
                return ZS_IOERROR;
        }

        ret = zs_prepare_val_buf(val, vallen, &valbuf, &valbuflen);
        if (ret != ZS_OK) {
                return ZS_IOERROR;
        }

        /* Get the current mappedfile size */
        ret = mappedfile_size(&f->mf, &mfsize);
        if (ret) {
                zslog(LOGDEBUG, "Could not get mappedfile size\n");
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
        xfree(keybuf);
        xfree(valbuf);

        return ret;
}

int zs_file_write_commit_record(struct zsdb_file *f)
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

                lc.type1 = REC_TYPE_LONG_COMMIT;
                lc.length = f->mf->crc32_data_len;
                lc.type2 = REC_TYPE_LONG_COMMIT;

                /* Compute CRC32 */
                lccrc = crc32(0L, Z_NULL, 0);
                lccrc = crc32(lccrc, (void *)&lc,
                              sizeof(struct zs_long_commit) - sizeof(uint32_t));
                crc = crc32_end(&f->mf);
                lc.crc32 = crc32_combine(crc, lccrc, sizeof(uint32_t));

                /* Create long commit */
                val = ((uint64_t)lc.type1 & ((1ULL << 56) - 1));
                write_be64(ptr + pos, val);
                pos += sizeof(uint64_t);

                write_be64(ptr + pos, lc.length);
                pos += sizeof(uint64_t);

                val = 0;
                val = ((uint64_t)lc.crc32 & ((1UL << 32) - 1)); /* crc */
                val |= ((uint64_t)lc.type2 << 56);               /* type */
                write_be64(ptr + pos, val);
                pos += sizeof(uint64_t);

                buflen = ZS_LONG_COMMIT_REC_SIZE;
        } else {
                /* Short commit */
                uint64_t val = 0;
                uint32_t sccrc;
                struct zs_short_commit sc;

                sc.type = REC_TYPE_COMMIT;
                sc.length = f->mf->crc32_data_len;

                /* Compute CRC32 */
                sccrc = crc32(0L, Z_NULL, 0);
                sccrc = crc32(sccrc, (void *)&sc,
                              sizeof(struct zs_short_commit) - sizeof(uint32_t));
                crc = crc32_end(&f->mf);
                sc.crc32 = crc32_combine(crc, sccrc, sizeof(uint32_t));

                val = ((uint64_t)sc.crc32 & ((1UL << 32) - 1)); /* crc */
                val |= ((uint64_t)sc.length << 32);             /* length */
                val |= ((uint64_t)sc.type << 56);               /* type */
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
                                unsigned char *key, size_t keylen)
{
        int ret = ZS_OK;
        unsigned char *dbuf;
        size_t dbuflen, mfsize, nbytes;

        ret = zs_prepare_delete_key_buf(key, keylen, &dbuf, &dbuflen);
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
        xfree(dbuf);
        return ret;
}
