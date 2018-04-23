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

#include "btree.h"
#include "cstring.h"
#include "file-lock.h"
#include "htable.h"
#include "list.h"
#include "macros.h"
#include "mappedfile.h"
#include "pqueue.h"
#include "util.h"
#include "vecu64.h"

#include <uuid/uuid.h>

CPP_GUARD_START

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
#define UUID_STRLEN  37

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
        char uuidstr[37];
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
        REC_TYPE_LONG_DELETED        = REC_TYPE_LONG | REC_TYPE_LONG,
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


/* File Data */
struct zsdb_file {
        struct list_head list;
        enum db_ftype_t type;
        struct zs_header header;
        cstring fname;
        struct mappedfile *mf;
        struct vecu64 *index;
        /* TODO: Fix the indexpos type int to uint64_t.
           Here and in struct vecu64 (count)
         */
        int indexpos;           /* Position in the index vec */
        int is_open;
        uint64_t priority;      /* Higher the number, higher the priority */
        int dirty;
};

struct zsdb_files {
        struct zsdb_file factive; /* The active file */
        struct list_head pflist;  /* The list of packed files */
        struct list_head fflist;  /* The list of finalised files */
        unsigned int afcount;     /* Number of active files - should be 1 */
        unsigned int pfcount;     /* Number of packed files */
        unsigned int ffcount;     /* Number of finalised files */
};

/* Storage Backend */
typedef enum _zsdb_be_t {
        ZSDB_BE_ACTIVE,
        ZSDB_BE_FINALISED,
        ZSDB_BE_PACKED,
} zsdb_be_t;

struct zsdb_store {
        zsdb_be_t type;
};

struct iter_key_data {
        unsigned char *key;
        size_t len;
};

/* Data for priority queue for lookup */
struct txn_data {
        zsdb_be_t type;
        int priority;
        int done;
        union {
                btree_iter_t iter;
                struct zsdb_file *f;
        } data;
};

/* Transaction structure */
struct txn_htable {
        struct htable table;
};

struct txn_htable_entry {
        struct htable_entry entry;
        unsigned char *key;
        size_t keylen;
        void *value;
};

struct txn {
        struct zsdb *db;
        struct pqueue pq;
        struct txn_htable ht;

        struct txn_data **datav;
        int txn_data_count;
        int txn_data_alloc;

        int forone_txn;
};

/* Transaction types */
enum TxnType {
        TXN_ALL,
        TXN_PACKED_ONLY,
};


/* Private data structure */
struct zsdb_priv {
        uuid_t uuid;              /* The UUID for the DB */
        cstring dotzsdbfname;     /* The filename, with path of .zsdb */
        struct dotzsdb dotzsdb;   /* .zsdb contents */
        ino_t dotzsdb_ino;        /* The inode number of of the .zsdb file
                                   * when opened.
                                   */
        cstring dbdir;            /* The directory path */

        struct zsdb_files dbfiles;

        /* Locks */
        struct file_lock wlk;     /* Lock when writing */
        struct file_lock plk;     /* Lock when packing */

        struct btree *memtree;    /* in-memory B-Tree */
        struct btree *fmemtree;   /* in-memory B-Tree of finalised records */

        int open;                 /* is the db open */
        int flags;                /* The flags passed during call to open */
        int dbdirty;              /* Marked dirty when there are changes
                                   * (add/remove/pack) to the db */
};

/* zeroskip-active.c */
extern int zs_active_file_open(struct zsdb_priv *priv, uint32_t idx, int mode);
extern int zs_active_file_close(struct zsdb_priv *priv);
extern int zs_active_file_finalise(struct zsdb_priv *priv);
extern int zs_active_file_write_keyval_record(struct zsdb_priv *priv,
                                              unsigned char *key, size_t keylen,
                                              unsigned char *val, size_t vallen);
extern int zs_active_file_write_commit_record(struct zsdb_priv *priv);
extern int zs_active_file_write_delete_record(struct zsdb_priv *priv,
                                              unsigned char *key,
                                              size_t keylen);
extern int zs_active_file_record_foreach(struct zsdb_priv *priv,
                                         foreach_cb *cb, void *cbdata);
extern int zs_active_file_new(struct zsdb_priv *priv, uint32_t idx);

/* zeroskip-dotzsdb.c */
extern int zs_dotzsdb_create(struct zsdb_priv *priv);
extern int zs_dotzsdb_validate(struct zsdb_priv *priv);
extern int zs_dotzsdb_update_index(struct zsdb_priv *priv, uint32_t idx);
extern ino_t zs_dotzsdb_get_ino(struct zsdb_priv *priv);
extern int zs_dotzsdb_update_begin(struct zsdb_priv *priv);
extern int zs_dotzsdb_update_end(struct zsdb_priv *priv);

/* zeroskip-file.c */
extern int zs_file_write_keyval_record(struct zsdb_file *f,
                                       unsigned char *key, size_t keylen,
                                       unsigned char *val, size_t vallen);
extern int zs_file_write_commit_record(struct zsdb_file *f, int final);
extern int zs_file_write_delete_record(struct zsdb_file *f,
                                       unsigned char *key, size_t keylen);

/* zeroskip-filename.c */
extern void zs_filename_generate_active(struct zsdb_priv *priv, cstring *fname);
extern void zs_filename_generate_packed(struct zsdb_priv *priv, cstring *fname,
                                        uint32_t startidx, uint32_t endidx);

/* zeroskip-finalised.c */
extern int zs_finalised_file_open(const char *path, struct zsdb_file **fptr);
extern int zs_finalised_file_close(struct zsdb_file **fptr);
extern int zs_finalised_file_record_foreach(struct zsdb_file *f,
                                            foreach_cb *cb, void *cbdata);

/* zeroskip-header.c */
extern int zs_header_write(struct zsdb_file *f);
extern int zs_header_validate(struct zsdb_file *f);

/* zeroskip-packed.c */
extern int zs_packed_file_open(const char *path, struct zsdb_file **fptr);
extern int zs_packed_file_close(struct zsdb_file **fptr);
extern int zs_packed_file_new(const char *path,
                              uint32_t startidx, uint32_t endidx,
                              struct zsdb_file **fptr);
extern int zs_packed_file_new_from_memtree(const char *path,
                                           uint32_t startidx,
                                           uint32_t endidx,
                                           struct zsdb_priv *priv,
                                           struct zsdb_file **fptr);
extern int zs_packed_file_write_record(struct record *record, void *data);
extern int zs_packed_file_write_commit_record(struct zsdb_file *f);
extern int zs_pq_cmp_key_frm_offset(const void *d1, const void *d2,
                                    void *cbdata);
extern int zs_packed_file_get_key_from_offset(struct zsdb_file *f,
                                              unsigned char **key,
                                              uint64_t *len);
extern int zs_packed_file_bsearch_index(const unsigned char *key,
                                        const size_t keylen,
                                        struct zsdb_file *f, uint64_t *location,
                                        unsigned char **value, size_t *vallen);

/* zeroskip-record.c */
extern int zs_record_read_from_file(struct zsdb_file *f, size_t *offset,
                                    foreach_cb *cb, void *cbdata);
extern int zs_record_read_key_from_file_offset(struct zsdb_file *f,
                                               size_t offset,
                                               struct zs_key *key);
extern int zs_read_key_val_record_from_file_offset(struct zsdb_file *f,
                                                   size_t *offset,
                                                   struct zs_key *key,
                                                   struct zs_val *val);

/* zeroskip-transaction.c */
extern int zs_transaction_new(struct zsdb *db, struct txn **txn);
extern int zs_transaction_begin(struct txn **txn, enum TxnType type);
extern int zs_transaction_begin_at_key(struct txn **txn,
                                       unsigned char *key,
                                       size_t keylen,
                                       int *found,
                                       unsigned char **value,
                                       size_t *vallen);
extern struct txn_data *zs_transaction_get(struct txn *txn);
extern int zs_transaction_next(struct txn *txn,
                               struct txn_data *data);
extern void zs_transaction_end(struct txn **txn);
CPP_GUARD_END
#endif  /* _ZEROSKIP_PRIV_H_ */
