/*
 * zeroskip
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#ifndef _ZEROSKIP_H_
#define _ZEROSKIP_H_

#include <libzeroskip/macros.h>
#include <libzeroskip/btree.h>

#include <stdio.h>
#include <stdint.h>

CPP_GUARD_START

typedef enum {
        DB_DUMP_ACTIVE,
        DB_DUMP_ALL,
} DBDumpLevel;

#define MODE_RDWR         0           /* Open for reading/writing */
#define MODE_CREATE       1           /* Mode for creating */
#define MODE_CUSTOMSEARCH 2           /* Use custom search function */

/* Return codes */
enum {
        ZS_OK             =  0,
        ZS_DONE           =  1,
        ZS_IOERROR        = -1,
        ZS_AGAIN          = -2,
        ZS_EXISTS         = -3,
        ZS_INTERNAL       = -4,
        ZS_NOTFOUND       = -5,
        ZS_NOTIMPLEMENTED = -6,
        ZS_FULL           = -7,
        ZS_ERROR          = -8,
        ZS_INVALID_DB     = -9,
        ZS_NOMEM          = -10,
        ZS_INVALID_MODE   = -11,
        ZS_NOT_OPEN       = -12,
        ZS_INVALID_FILE   = -13,
};

/* Log levels */
enum {
        ZSLOGDEBUG   = 0,
        ZSLOGVERBOSE = 1,
        ZSLOGNOTICE  = 2,
        ZSLOGWARNING = 3,
};

/* Transactions */
struct zsdb_txn;

/* DB Iterator */
struct zsbd_iter;

/*
 * Callbacks
 */
typedef int zsdb_foreach_p(void *data,
                       const unsigned char *key, size_t keylen,
                       const unsigned char *value, size_t vallen);

typedef int zsdb_foreach_cb(void *data,
                       const unsigned char *key, size_t keylen,
                       const unsigned char *value, size_t vallen);

typedef int (*zsdb_cmp_fn)(const unsigned char *s1, size_t l1,
                           const unsigned char *s2, size_t l2);

/*
 * The main Zeroskip structure
 */
struct zsdb {
        struct zsdb_iter *iter;     /* All open iterators */
        unsigned int numtrans;      /* Total number of open transactions */
        void *priv;                 /* Private */
};

/*
 * Zeroskip API
 */
extern int zsdb_init(struct zsdb **pdb, zsdb_cmp_fn dbcmpfn,
                     btree_search_cb_t btcmpfn);
extern void zsdb_final(struct zsdb **pdb);
extern int zsdb_open(struct zsdb *db, const char *dbdir, int mode);
extern int zsdb_close(struct zsdb *db);
extern int zsdb_add(struct zsdb *db, const unsigned char *key, size_t keylen,
                    const unsigned char *value, size_t vallen,
                    struct zsdb_txn **txn);
extern int zsdb_remove(struct zsdb *db, const unsigned char *key,
                       size_t keylen, struct zsdb_txn **txn);
extern int zsdb_commit(struct zsdb *db, struct zsdb_txn **txn);
extern int zsdb_fetch(struct zsdb *db, const unsigned char *key, size_t keylen,
                      const unsigned char **value, size_t *vallen,
                      struct zsdb_txn **txn);
extern int zsdb_fetchnext(struct zsdb *db,
                          const unsigned char *key, size_t keylen,
                          const unsigned char **found, size_t *foundlen,
                          const unsigned char **value, size_t *vallen,
                          struct zsdb_txn **txn);
extern int zsdb_foreach(struct zsdb *db, const unsigned char *prefix,
                        size_t prefixlen,
                        zsdb_foreach_p *p, zsdb_foreach_cb *cb, void *cbdata,
                        struct zsdb_txn **txn);
extern int zsdb_forone(struct zsdb *db, const unsigned char *key, size_t keylen,
                       zsdb_foreach_p *p, zsdb_foreach_cb *cb, void *cbdata,
                       struct zsdb_txn **txn);
extern int zsdb_abort(struct zsdb *db, struct zsdb_txn **txn);
extern int zsdb_consistent(struct zsdb *db, struct zsdb_txn **txn);
extern int zsdb_dump(struct zsdb *db, DBDumpLevel level);
extern int zsdb_repack(struct zsdb *db);
extern int zsdb_info(struct zsdb *db);
extern int zsdb_finalise(struct zsdb *db);

extern int zsdb_transaction_begin(struct zsdb *db, struct zsdb_txn **txn);
extern void zsdb_transaction_end(struct zsdb_txn **txn);

/* locking routines */
extern int zsdb_write_lock_acquire(struct zsdb *db, long timeout_ms);
extern int zsdb_write_lock_release(struct zsdb *db);
extern int zsdb_write_lock_is_locked(struct zsdb *db);
extern int zsdb_pack_lock_acquire(struct zsdb *db, long timeout_ms);
extern int zsdb_pack_lock_release(struct zsdb *db);
extern int zsdb_pack_lock_is_locked(struct zsdb *db);

CPP_GUARD_END
#endif  /* _ZEROSKIP_H_ */
