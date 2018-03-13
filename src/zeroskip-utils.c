/*
 * zeroskip-utils.c
 * Convenience functions that are used across zsdb
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 */

#include <stdio.h>

#include "zeroskip.h"
#include "zeroskip-priv.h"

/* Caller should free buf
 */
int zs_prepare_key_buf(unsigned char *key, size_t keylen,
                       unsigned char **buf, size_t *buflen)
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

        kbuf = xcalloc(1, kbuflen);

        if (type == REC_TYPE_KEY) {
                /* If it is a short key, the first 3 fields make up 64 bits */
                uint64_t val;
                val = ((uint64_t)kbuflen & ((1ULL << 40) - 1)); /* Val offset */
                val |= ((uint64_t)keylen << 40);     /* Key length */
                val |= ((uint64_t)type << 56);       /* Type */
                write_be64(kbuf + pos, val);
                pos += sizeof(uint64_t);
                write_be64(kbuf + pos, 0ULL);     /* Extended length */
                pos += sizeof(uint64_t);
                write_be64(kbuf + pos, 0ULL);     /* Extended Value offset */
                pos += sizeof(uint64_t);

        } else {
                /* A long key has the type followed by 56 bits of nothing */
                uint64_t val;
                val = ((uint64_t)type & ((1ULL << 56) - 1));
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
int zs_prepare_val_buf(unsigned char *val, size_t vallen,
                       unsigned char **buf, size_t *buflen)
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

        vbuf = xcalloc(1, vbuflen);

        if (type == REC_TYPE_VALUE) {
                /* The first 3 fields in a short key make up 64 bits */
                uint64_t val = 0;
                val = ((uint64_t)vallen & ((1UL << 32) - 1));  /* Val length */
                val |= ((uint64_t)type << 56);                 /* Type */
                write_be64(vbuf + pos, val);
                pos += sizeof(uint64_t);
                write_be64(vbuf + pos, 0ULL);     /* Extended length */
                pos += sizeof(uint64_t);
        } else {
                /* A long val has the type followed by 56 bits of nothing */
                uint64_t val;
                val = ((uint64_t)type & ((1UL << 56) - 1));
                write_be64(vbuf + pos, val);
                pos += sizeof(uint64_t);
                write_be64(vbuf + pos, vallen);
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
int zs_prepare_delete_key_buf(unsigned char *key, size_t keylen,
                              unsigned char **buf, size_t *buflen)
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

        kbuf = xcalloc(1, kbuflen);

        if (keylen <= MAX_SHORT_KEY_LEN) {
                val = ((uint64_t)0ULL & ((1ULL << 40) - 1));
                val |= ((uint64_t)keylen << 40);     /* Key length */
                val |= ((uint64_t)type << 56);       /* Type */
                write_be64(kbuf + pos, val);
                pos += sizeof(uint64_t);
                write_be64(kbuf + pos, 0ULL);     /* Extended length */
                pos += sizeof(uint64_t);
                write_be64(kbuf + pos, 0ULL);     /* Extended Value offset */
                pos += sizeof(uint64_t);
        } else {
                type = REC_TYPE_LONG_DELETED;
                val = ((uint64_t)type & ((1ULL << 56) - 1));
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
