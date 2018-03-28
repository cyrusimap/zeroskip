/*
 * zeroskip
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */
#include "log.h"
#include "util.h"
#include "zeroskip.h"
#include "zeroskip-priv.h"

/*
 * Private functions
 */
static int zs_read_key_rec(struct zsdb_file *f, size_t *offset,
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
                key->base.slen = data >> 40;
                key->base.sval_offset = data & ((1ULL >> 40) - 1);
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
                val->base.slen = data & ((1UL >> 32) - 1);
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

static int zs_read_key_val_record(struct zsdb_file *f, size_t *offset,
                                  foreach_cb *cb, void *cbdata)
{
        int ret = ZS_OK;
        struct zs_key key;
        struct zs_val val;
        size_t keylen, vallen;


        zs_read_key_rec(f, offset, &key);

        *offset += (key.base.type == REC_TYPE_KEY) ?
                key.base.sval_offset : key.base.lval_offset;

        zs_read_val_rec(f, offset, &val);

        keylen = (key.base.type == REC_TYPE_KEY) ?
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
                                  foreach_cb *cb, void *cbdata)
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

/*
 * Public functions
 */

/*
 * zs_record_read_from_file():
 * Reads a record from a given struct zsdb_file
 */
int zs_record_read_from_file(struct zsdb_file *f, size_t *offset,
                             foreach_cb *cb, void *cbdata)
{
        unsigned char *bptr, *fptr;
        uint64_t data;
        enum record_t rectype;

        if (!f->is_open)
                return ZS_IOERROR;

        bptr = f->mf->ptr;
        fptr = bptr + *offset;

        data = read_be64(fptr);
        rectype = data >> 56;

        switch(rectype) {
        case REC_TYPE_KEY:
        case REC_TYPE_LONG_KEY:
                zs_read_key_val_record(f, offset, cb, cbdata);
                break;
        case REC_TYPE_VALUE:
        case REC_TYPE_LONG_VALUE:
                zslog(LOGWARNING, "Control shouldn't reach here.\n");
                break;
        case REC_TYPE_COMMIT:
                *offset = *offset + ZS_SHORT_COMMIT_REC_SIZE;
                break;
        case REC_TYPE_LONG_COMMIT:
                *offset = *offset + ZS_LONG_COMMIT_REC_SIZE;
                break;
        case REC_TYPE_2ND_HALF_COMMIT:
                break;
        case REC_TYPE_FINAL:
                break;
        case REC_TYPE_LONG_FINAL:
                break;
        case REC_TYPE_DELETED:
        case REC_TYPE_LONG_DELETED:
                zs_read_deleted_record(f, offset, cb, cbdata);
                break;
        case REC_TYPE_UNUSED:
                break;
        default:
                break;
        }

        return ZS_OK;
}

/*
 * zs_record_read_key_from_file_offset():
 * Reads a key from a given struct zsdb_file offset. This is primarily used in
 * the PQ comparison packed file records.
 */
int zs_record_read_key_from_file_offset(struct zsdb_file *f, size_t offset,
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
