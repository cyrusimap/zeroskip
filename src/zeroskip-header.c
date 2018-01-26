/*
 * zeroskip-header.c : zeroskip header manipulation
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#include "log.h"
#include "zeroskip.h"
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
        struct zs_header hdr;
        size_t nbytes;
        uint32_t crc;

        if (!f->is_open) {
                return ZS_ERROR;
        }

        crc =crc32(0L, Z_NULL, 0);

        /* XXX: The copying should be done to an unsigned char buffer */
        hdr.signature = hton64(f->header.signature);
        hdr.version = hton32(f->header.version);
        memcpy(hdr.uuid, f->header.uuid, sizeof(uuid_t));
        hdr.startidx = htonl(f->header.startidx);
        hdr.endidx = htonl(f->header.endidx);

        /* compute the crc32 of the the fields of the header minus the
           crc32 field */
        crc = crc32(crc, (void *)&f->header,
                    sizeof(f->header) - sizeof(f->header.crc32));
        hdr.crc32 = htonl(crc);

        ret = mappedfile_write(&f->mf, (void *)&hdr, sizeof(hdr), &nbytes);
        if (ret) {
                zslog(LOGDEBUG, "Error writing header\n");
                goto done;
        }

        ret = mappedfile_flush(&f->mf);
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
        mappedfile_seek(&f->mf, 0, 0);

        mappedfile_size(&f->mf, &mfsize);
        if (mfsize < ZS_HDR_SIZE) {
                zslog(LOGDEBUG, "File too small to be a zeroskip DB\n");
                return ZS_INVALID_DB;
        }

        phdr = (struct zs_header *)f->mf->ptr;

        /* Signature */
        if (phdr->signature != hton64(ZS_SIGNATURE)) {
                zslog(LOGDEBUG, "Invalid signature on Zeroskip DB!\n");
                return ZS_INVALID_DB;
        }
        f->header.signature = ntoh64(phdr->signature);

        /* Version */
        version = ntohl(phdr->version);
        if (version == 1) {
                zslog(LOGDEBUG, "Valid zeroskip DB file. Version: %d\n",
                        version);
        } else {
                zslog(LOGDEBUG, "Invalid zeroskip DB version.\n");
        }
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
        crc = crc32(crc, (void *)&f->header,
                    sizeof(struct zs_header) - sizeof(uint32_t));
        if (crc != f->header.crc32) {
                zslog(LOGDEBUG, "checksum failed for zeroskip header.\n");
                return ZS_INVALID_DB;
        }

        return ZS_OK;
}
