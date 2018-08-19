/*
 * zeroskip-header.c : zeroskip header manipulation
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#include <libzeroskip/log.h>
#include <libzeroskip/mfile.h>
#include <libzeroskip/zeroskip.h>
#include "zeroskip-priv.h"

#include <stdint.h>
#include <string.h>
#include <zlib.h>

/* zs_header_write():
 *  Write the header to a mapped file that is open, the values
 *  of the header are got from f->header. So the caller must ensure
 *  the fields are valid.
 *
 * Return:
 *   This function will return an error if the file isn't open
 *   or if for some reason the write itself failed.
 *   ZS_OK on success.
 */
int zs_header_write(struct zsdb_file *f)
{
        int ret = ZS_OK;
        size_t nbytes;
        uint32_t crc;
        unsigned char stackbuf[ZS_HDR_SIZE];
        unsigned char *sptr;

        memset(&stackbuf, 0, ZS_HDR_SIZE);
        sptr = stackbuf;

        if (!f->is_open) {
                return ZS_ERROR;
        }

        crc = crc32(0L, Z_NULL, 0);

        /* Signature */
        memcpy(sptr, &f->header.signature, sizeof(uint64_t));
        sptr += sizeof(uint64_t);

        /* Version */
        *((uint32_t *)sptr) = hton32(f->header.version);
        sptr += sizeof(uint32_t);

        /* UUID */
        memcpy(sptr, f->header.uuid, sizeof(uuid_t));
        sptr += sizeof(uuid_t);

        /* Start Index */
        *((uint32_t *)sptr) = hton32(f->header.startidx);
        sptr += sizeof(uint32_t);

        /* End Index */
        *((uint32_t *)sptr) = hton32(f->header.endidx);
        sptr += sizeof(uint32_t);

        /* compute the crc32 of the the fields of the header minus the
           crc32 field */

        crc = crc32(0L, Z_NULL, 0);
        crc = crc32(crc, (void *)&f->header.signature, sizeof(uint64_t));
        crc = crc32(crc, (void *)&f->header.version, sizeof(uint32_t));
        crc = crc32(crc, (void *)&f->header.uuid, sizeof(uuid_t));
        crc = crc32(crc, (void *)&f->header.startidx, sizeof(uint32_t));
        crc = crc32(crc, (void *)&f->header.endidx, sizeof(uint32_t));
        *((uint32_t *)sptr) = hton32(crc);
        sptr += sizeof(uint32_t);

        ret = mfile_write(&f->mf, &stackbuf, ZS_HDR_SIZE, &nbytes);
        if (ret) {
                zslog(LOGDEBUG, "Error writing header\n");
                goto done;
        }

        ret = mfile_flush(&f->mf);
        if (ret) {
                /* TODO: try again before giving up */
                zslog(LOGDEBUG, "Error flushing data to disk.\n");
                goto done;
        }

done:
        return ret;
}

/* zs_header_validate():
 *  Check if the header of a mapped file is valid.
 *
 * Return:
 *  This function will return an error if the mapped file isn't open
 *  or if the header is invalid.
 *  If the mapped file contains a valid header, this function populates the
 * f->header field with the values.
 */
int zs_header_validate(struct zsdb_file *f)
{
        struct zs_header *phdr;
        uint32_t version;
        uint32_t crc;
        size_t mfsize;

        if (!f->is_open || (f->mf->fd < 0))
                return ZS_ERROR;

        /* Seek to the beginning of the mapped file */
        mfile_seek(&f->mf, 0, 0);

        mfile_size(&f->mf, &mfsize);
        if (mfsize < ZS_HDR_SIZE) {
                zslog(LOGDEBUG, "File too small to be a zeroskip DB\n");
                return ZS_INVALID_DB;
        }

        phdr = (struct zs_header *)f->mf->ptr;

        /* Signature */
        if (phdr->signature != ZS_SIGNATURE) {
                zslog(LOGDEBUG, "Invalid signature on Zeroskip DB!\n");
                return ZS_INVALID_DB;
        }
        f->header.signature = phdr->signature;

        /* Version */
        version = ntoh32(phdr->version);
        #if 0
        if (version == 1) {
                zslog(LOGDEBUG, "Valid zeroskip DB file(%s). Version: %d\n",
                      f->fname.buf, version);
        } else {
                zslog(LOGDEBUG, "Invalid zeroskip DB version.\n");
        }
        #endif
        f->header.version = version;

        /* UUID */
        memcpy(f->header.uuid, phdr->uuid, sizeof(uuid_t));

        /* Start and end indices */
        f->header.startidx = ntoh32(phdr->startidx);
        f->header.endidx = ntoh32(phdr->endidx);

        /* CRC32 */
        f->header.crc32 = ntoh32(phdr->crc32);

        /* Check CRC32 */
        crc = crc32(0L, Z_NULL, 0);
        crc = crc32(crc, (void *)&f->header.signature, sizeof(uint64_t));
        crc = crc32(crc, (void *)&f->header.version, sizeof(uint32_t));
        crc = crc32(crc, (void *)&f->header.uuid, sizeof(uuid_t));
        crc = crc32(crc, (void *)&f->header.startidx, sizeof(uint32_t));
        crc = crc32(crc, (void *)&f->header.endidx, sizeof(uint32_t));

        if (crc != f->header.crc32) {
                zslog(LOGDEBUG, "Checksum failed for zeroskip header: %u, expected: %u.\n",
                      crc, f->header.crc32);
                return ZS_INVALID_DB;
        }

        return ZS_OK;
}
