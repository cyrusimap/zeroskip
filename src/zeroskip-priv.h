/*
 * zeroskip-priv.h
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#ifndef _ZEROSKIP_PRIV_H_
#define _ZEROSKIP_PRIV_H_

#include "cstring.h"
#include "mappedfile.h"
#include "util.h"

#include <uuid/uuid.h>

/* Sizes */
#define MB       (1 << 20)
#define TWOMB    (2 << 20)
#define THREEMB  (3 << 20)
#define FOURMB   (4 << 20)

/*
 * Zeroskip db files have the following file naming scheme:
 *   zeroskip-$(UUID)-$(index)                     - for an unpacked file
 *   zeroskip-$(UUID)-$(startindex)-$(endindex)    - for a packed filed
 *
 * The UUID, startindex and endindex values are in the header of each file.
 * The index starts with a 0, for a completely new Zeroskip DB. And is
 * incremented every time a file is finalsed(packed).
 */
#define ZS_FNAME_PREFIX       "zeroskip-"
#define ZS_FNAME_PREFIX_LEN   9
#define ZS_SIGNATURE          0x5a45524f534b4950 /* "ZEROSKIP" */
#define ZS_VERSION            1

/* This is the size of the unparssed uuid string */
#define UUID_STRLEN  36

/**
 * zeroskip header.
 */
/* Header offsets */
enum {
        ZS_HDR      = 0,
        ZS_VER      = 8,
        ZS_UUID     = 12,
        ZS_SIDX     = 28,
        ZS_EIDX     = 32,
        ZS_CRC      = 36,
};

struct zs_header {
        uint64_t signature;         /* Signature */
        uint32_t version;           /* Version Number */
        uuid_t   uuid;              /* UUID of DB - 128 bits: unsigned char uuid_t[16];*/
        uint32_t startidx;          /* Start Index of DB range */
        uint32_t endidx;            /* End Index of DB range */
        uint32_t crc32;             /* CRC32 of rest of header */
};

#define ZS_HDR_SIZE      sizeof(struct zs_header)


/**
 * Zeroskip .zsdb
 */
struct dotzsdb {
        uint64_t signature;
        uint32_t curidx;
        char uuidstr[36];
};                              /* A total of 48 bytes */
#define DOTZSDB_FNAME ".zsdb"
#define DOTZSDB_SIZE  sizeof(struct dotzsdb)



/* Types of files in the DB */
enum db_ftype_t {
        DB_FTYPE_ACTIVE,
        DB_FTYPE_FINALISED,
        DB_FTYPE_PACKED,
        DB_FTYPE_UNKNOWN,
};

/**
 * Zeroskip record[key|value|commit]
 */
enum record_t {
        REC_TYPE_UNUSED              =  0,
        REC_TYPE_KEY                 =  1,
        REC_TYPE_VALUE               =  2,
        REC_TYPE_COMMIT              =  4,
        REC_TYPE_2ND_HALF_COMMIT     =  8,
        REC_TYPE_FINAL               = 16,
        REC_TYPE_LONG                = 32,
        REC_TYPE_DELETED             = 64,
        REC_TYPE_LONG_KEY            = REC_TYPE_KEY | REC_TYPE_LONG,
        REC_TYPE_LONG_VALUE          = REC_TYPE_VALUE | REC_TYPE_LONG,
        REC_TYPE_LONG_COMMIT         = REC_TYPE_COMMIT | REC_TYPE_LONG,
        REC_TYPE_LONG_FINAL          = REC_TYPE_FINAL | REC_TYPE_LONG,
};

struct zs_key_base {
        uint8_t  type;
        uint16_t slen;
        uint64_t sval_offset : 40;
        uint64_t llen;
        uint64_t lval_offset;
};
/* Since the structure isn't packed not doing a sizeof */
#define ZS_KEY_BASE_REC_SIZE 24

struct zs_key {
        struct zs_key_base base;
        unsigned char *data;
};

struct zs_val_base {
        uint8_t  type;
        uint32_t slen : 24;
        uint32_t nullpad;
        uint64_t llen;
};
/* Since the structure isn't packed not doing a sizeof */
#define ZS_VAL_BASE_REC_SIZE 16

struct zs_val {
        struct zs_val_base base;
        unsigned char *data;
};

struct zs_short_commit {
        uint8_t type;
        uint32_t length : 24;
        uint32_t crc32;
};
/* Since the structure isn't packed not doing a sizeof */
#define ZS_SHORT_COMMIT_REC_SIZE 8

struct zs_long_commit {
        uint8_t  type1;
        uint64_t padding1 : 56;
        uint64_t length;
        uint8_t  type2;
        uint32_t padding2 : 24;
        uint32_t crc32;
};
/* Since the structure isn't packed not doing a sizeof */
#define ZS_LONG_COMMIT_REC_SIZE 24

#define MAX_SHORT_KEY_LEN 65535
#define MAX_SHORT_VAL_LEN 16777215

#ifdef ZS_DEBUG
extern int zsdb_break(int err);
extern void assert_zsdb(struct zsdb *db);
#else
#define zsdb_break(x) (x)
#endif

/* Storage Backend */
typedef enum _zsdb_be_t {
        ZSDB_BE_MEM,
        ZSDB_BE_LOG,
        ZSDB_BE_PACK,
} zsdb_be_t;

struct zsdb_store {
        zsdb_be_t type;
};

/* Private data structure */
struct zsdb_priv {
        uuid_t uuid;            /* The UUID for the DB */
        struct dotzsdb dotzsdb; /* .zsdb contents */
        cstring dbdir;          /* The directory path */

        int nopen;              /* count of opens on this instance */
};

/* zeroskip-dotzsdb.c */
extern int zs_dotzsdb_create(struct zsdb_priv *priv);
extern int zs_dotzsdb_validate(struct zsdb_priv *priv);
extern int zs_dotzsdb_update_index(struct zsdb_priv *priv, uint32_t idx);

#endif  /* _ZEROSKIP_PRIV_H_ */
