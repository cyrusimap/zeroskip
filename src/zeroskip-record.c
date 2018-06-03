/*
 * zeroskip
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#include <libzeroskip/log.h>
#include <libzeroskip/util.h>
#include <libzeroskip/zeroskip.h>
#include "zeroskip-priv.h"

#include <zlib.h>

/*
 * Private functions
 */

static int zs_record_read_key(struct zsdb_file *f, size_t *offset,
                              const unsigned char **key, size_t *keylen)
{
        unsigned char *bptr;
        unsigned char *fptr;
        uint64_t data;
        uint8_t type;

        bptr = f->mf->ptr;
        fptr = bptr + *offset;

        data = read_be64(fptr);
        type = data >> 56;

        if (type == REC_TYPE_KEY || type == REC_TYPE_DELETED) {
                *keylen = data >> 40 & 0xFFFFFF;
                *offset = data & 0xFFFFFFFF;
        } else if (type == REC_TYPE_LONG_KEY || type == REC_TYPE_LONG_DELETED) {
                *keylen = read_be64(fptr + 8);
                *offset = read_be64(fptr + 16);
        }

        *key = fptr + ZS_KEY_BASE_REC_SIZE;

        return ZS_OK;
}

static int zs_record_read_val(struct zsdb_file *f, size_t *offset,
                              const unsigned char **val, size_t *vallen)
{
        unsigned char *bptr;
        unsigned char *fptr;
        uint64_t data;
        uint8_t type;

        bptr = f->mf->ptr;
        fptr = bptr + *offset;

        data = read_be64(fptr);
        type = data >> 56;

        if (type == REC_TYPE_VALUE) {
                *vallen = (data >> 32) & 0xFFFFFF;
        } else if (type == REC_TYPE_LONG_VALUE){
                *vallen = read_be64(fptr + 8);
        }

        *val = fptr + ZS_VAL_BASE_REC_SIZE;

        return ZS_OK;
}

static int zs_read_key_rec(const struct zsdb_file *f,
                           size_t *offset,
                           struct zs_key *key)
{
        unsigned char *bptr;
        unsigned char *fptr;
        uint64_t data;

        bptr = f->mf->ptr;
        fptr = bptr + *offset;

        data = read_be64(fptr);
        key->base.type = data >> 56;

        if (key->base.type == REC_TYPE_KEY ||
            key->base.type == REC_TYPE_DELETED) {
                key->base.slen = data >> 40 & 0xFFFFFF;
                key->base.sval_offset = data & 0xFFFFFFFF;
                key->base.llen = 0;
                key->base.lval_offset = 0;
        } else if (key->base.type == REC_TYPE_LONG_KEY ||
                   key->base.type == REC_TYPE_LONG_DELETED) {
                key->base.slen = 0;
                key->base.sval_offset = 0;
                key->base.llen = read_be64(fptr + 8);
                key->base.lval_offset = read_be64(fptr + 16);
        }

        key->data = fptr + ZS_KEY_BASE_REC_SIZE;

        return ZS_OK;
}

static int zs_read_val_rec(struct zsdb_file *f, size_t *offset,
                           struct zs_val *val)
{
        unsigned char *bptr;
        unsigned char *fptr;
        uint64_t data;

        bptr = f->mf->ptr;
        fptr = bptr + *offset;

        data = read_be64(fptr);
        val->base.type = data >> 56;

        if (val->base.type == REC_TYPE_VALUE) {
                val->base.slen = (data >> 32) & 0xFFFFFF;
                val->base.nullpad = 0;
                val->base.llen = 0;
        } else if (val->base.type == REC_TYPE_LONG_VALUE) {
                val->base.slen = 0;
                val->base.nullpad = 0;
                val->base.llen = read_be64(fptr + 8);
        }

        val->data = fptr + ZS_VAL_BASE_REC_SIZE;

        return ZS_OK;
}

static int zs_read_key_val_record_cb(struct zsdb_file *f, size_t *offset,
                                     zsdb_foreach_cb *cb, void *cbdata)
{
        int ret = ZS_OK;
        struct zs_key key;
        struct zs_val val;
        size_t keylen, vallen;


        zs_read_key_rec(f, offset, &key);

        *offset += (key.base.type == REC_TYPE_KEY ||
                    key.base.type == REC_TYPE_DELETED) ?
                key.base.sval_offset : key.base.lval_offset;

        zs_read_val_rec(f, offset, &val);

        keylen = (key.base.type == REC_TYPE_KEY ||
                  key.base.type == REC_TYPE_DELETED) ?
                key.base.slen : key.base.llen;
        vallen = (val.base.type == REC_TYPE_VALUE) ?
                val.base.slen : val.base.llen;

        *offset += ZS_VAL_BASE_REC_SIZE +
                roundup64bits(vallen);

        if (cb) {
                cb(cbdata, key.data, keylen, val.data, vallen);
        }
        return ret;
}

static int zs_read_deleted_record(struct zsdb_file *f, size_t *offset,
                                  zsdb_foreach_cb *cb, void *cbdata)
{
        struct zs_key key;
        size_t keylen;

        zs_read_key_rec(f, offset, &key);

        keylen = (key.base.type == REC_TYPE_DELETED) ?
                key.base.slen : key.base.llen;

        *offset += ZS_KEY_BASE_REC_SIZE + roundup64bits(keylen);

        if (cb) {
                cb(cbdata, key.data, keylen, NULL, 0);
        }

        return ZS_OK;
}

static int zs_read_and_verify_commit_record(struct zsdb_file *f,
                                            size_t *offset)
{
        int ret = ZS_OK;
        unsigned char *bptr;
        unsigned char *fptr;
        uint64_t data;
        enum record_t rectype;
        uint32_t crc = 0;

        bptr = f->mf->ptr;
        fptr = bptr + *offset;

        data = read_be64(fptr);
        rectype = data >> 56;

        if (rectype == REC_TYPE_COMMIT) {
                unsigned char *rptr;
                struct zs_short_commit sc;
                uint64_t val;

                /* offset is the end of the short commit record */
                *offset = *offset + ZS_SHORT_COMMIT_REC_SIZE;

                crc = crc32(0L, Z_NULL, 0);

                sc.type = rectype;
                sc.length = (data >> 32) & 0xFFFFFF;
                sc.crc32 = data & 0xFFFFFFFF;

                /* Ddata begins at `rptr` */
                rptr = fptr - sc.length;

                /* The commit record without the CRC */
                val = data & 0xFFFFFFFF00000000;

                #if ZLIB_VERNUM == 0x12b0
                crc = crc32_z(crc, (void *)rptr, sc.length);
                crc = crc32_z(crc, (void *)&val, sizeof(uint64_t));
                #else
                crc = crc32(crc, (void *)rptr, sc.length);
                crc = crc32(crc, (void *)&val, sizeof(uint64_t));
                #endif

                if (sc.crc32 != crc) {
                        zslog(LOGWARNING, "Checksum failed for record at offset: %zu\n",
                                *offset);
                        ret = ZS_INVALID_DB;
                } else
                        ret = ZS_OK;

        } else if (rectype == REC_TYPE_LONG_COMMIT) {
                uint64_t val = 0;
                struct zs_long_commit lc;
                unsigned char *rptr;

                /* offset is the end of the long commit record */
                *offset = *offset + ZS_LONG_COMMIT_REC_SIZE;

                /* Begin computing */
                crc = crc32(0L, Z_NULL, 0);

                lc.type1 = rectype;
                lc.length = read_be64(fptr + sizeof(uint64_t));

                /* stored crc */
                val = read_be64(fptr + 2 * sizeof(uint64_t));
                lc.crc32 = val & 0xFFFFFFFF;

                /* Data begins at `rptr` */
                rptr = fptr - lc.length;

                val = 0;        /* 0 used to compute CRC */
                #if ZLIB_VERNUM == 0x12b0
                crc = crc32_z(crc, (void *)rptr, lc.length);
                crc = crc32_z(crc, (void *)&data, sizeof(uint64_t));
                crc = crc32_z(crc, (void *)lc.length, sizeof(uint64_t));
                crc = crc32_z(crc, (void *)&val, sizeof(uint64_t));
                #else
                crc = crc32(crc, (void *)rptr, lc.length);
                crc = crc32(crc, (void *)&data, sizeof(uint64_t));
                crc = crc32(crc, (void *)lc.length, sizeof(uint64_t));
                crc = crc32(crc, (void *)&val, sizeof(uint64_t));
                #endif

                if (lc.crc32 != crc) {
                        zslog(LOGWARNING, "Checksum failed for record at offset: %zu\n",
                                *offset);
                        ret = ZS_INVALID_DB;
                } else
                        ret = ZS_OK;
        } else {
                zslog(LOGDEBUG, "Invalid record\n");
                ret = ZS_INVALID_DB;
        }

        return ret;
}

/*
 * Public functions
 */

/*
 * zs_record_read_from_file():
 * Reads a record from a given struct zsdb_file
 */
int zs_record_read_from_file(struct zsdb_file *f, size_t *offset,
                             zsdb_foreach_cb *cb, zsdb_foreach_cb *deleted_cb,
                             void *cbdata)
{
        unsigned char *bptr, *fptr;
        uint64_t data;
        enum record_t rectype;
        int ret = ZS_OK;

        if (!f->is_open)
                return ZS_IOERROR;

        bptr = f->mf->ptr;
        fptr = bptr + *offset;

        data = read_be64(fptr);
        rectype = data >> 56;

        switch(rectype) {
        case REC_TYPE_KEY:
        case REC_TYPE_LONG_KEY:
                ret = zs_read_key_val_record_cb(f, offset, cb, cbdata);
                break;
        case REC_TYPE_VALUE:
        case REC_TYPE_LONG_VALUE:
                zslog(LOGWARNING, "Control shouldn't reach here.\n");
                break;
        case REC_TYPE_COMMIT:
        case REC_TYPE_LONG_COMMIT:
                ret = zs_read_and_verify_commit_record(f, offset);
                break;
        case REC_TYPE_2ND_HALF_COMMIT:
                break;
        case REC_TYPE_FINAL:
                break;
        case REC_TYPE_LONG_FINAL:
                break;
        case REC_TYPE_DELETED:
        case REC_TYPE_LONG_DELETED:
                zs_read_deleted_record(f, offset, deleted_cb, cbdata);
                break;
        case REC_TYPE_UNUSED:
                break;
        default:
                break;
        }

        return ret;
}

/*
 * zs_record_read_key_from_file_offset():
 * Reads a key from a given struct zsdb_file offset. This is primarily used in
 * the PQ comparison packed file records.
 */
int zs_record_read_key_from_file_offset(const struct zsdb_file *f,
                                        size_t offset,
                                        struct zs_key *key)
{
        unsigned char *bptr, *fptr;
        uint64_t data;
        enum record_t rectype;

        if (!f->is_open)
                return ZS_IOERROR;

        bptr = f->mf->ptr;
        fptr = bptr + offset;

        data = read_be64(fptr);
        rectype = data >> 56;

        switch(rectype) {
        case REC_TYPE_KEY:
        case REC_TYPE_LONG_KEY:
        case REC_TYPE_DELETED:
        case REC_TYPE_LONG_DELETED:
                zs_read_key_rec(f, &offset, key);
                break;
        default:
                return ZS_ERROR;
                break;
        }

        return ZS_OK;
}

int zs_read_key_val_record_from_file_offset(struct zsdb_file *f,
                                            size_t *offset,
                                            struct zs_key *key,
                                            struct zs_val *val)
{
        size_t vallen;

        zs_read_key_rec(f, offset, key);

        *offset += (key->base.type == REC_TYPE_KEY ||
                    key->base.type == REC_TYPE_DELETED) ?
                key->base.sval_offset : key->base.lval_offset;

        zs_read_val_rec(f, offset, val);

        vallen = (val->base.type == REC_TYPE_VALUE) ?
                val->base.slen : val->base.llen;

        *offset += ZS_VAL_BASE_REC_SIZE +
                roundup64bits(vallen);

        return ZS_OK;
}

int zs_record_read_key_val_from_offset(struct zsdb_file *f, size_t *offset,
                                       const unsigned char **key, size_t *keylen,
                                       const unsigned char **val, size_t *vallen)
{
        size_t dataoffset = *offset;

        zs_record_read_key(f, &dataoffset, key, keylen);

        zs_record_read_val(f, &dataoffset, val, vallen);

        return ZS_OK;
}
