/*
 * zeroskip
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#ifndef _ZEROSKIP_H_
#define _ZEROSKIP_H_

#include "macros.h"

#include <stdio.h>
#include <stdint.h>

CPP_GUARD_START

typedef enum {
        DB_DUMP_ACTIVE,
        DB_DUMP_ALL,
} DBDumpLevel;

#define MODE_RDWR   0           /* Open for reading/writing */
#define MODE_CREATE 1           /* Mode for creating */

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

/* Transactions */
struct txn;

/*
 * Operations structure for Zeroskip DB
 */
struct zsdb_operations {
        int (*xopen)(void);
        int (*xclose)(void);
        int (*xread)(void);
        int (*xwrite)(void);
        int (*xlock)(void);
        int (*xislocked)(void);
        int (*xcmp)(void *p1, size_t n1, void *p2, size_t n2);
};

/*
 * Zeroskip DB Iterator
 */
struct zsbd_iter;

struct zsdb {
        struct zsdb_iter *iter;     /* All open iterators */
        struct zsdb_operations *op; /* Operations */
        unsigned int numtrans;      /* Total number of open transactions */
        void *priv;                 /* Private */
};

/*
 * Callbacks
 */
typedef int foreach_p(void *data,
                       unsigned char *key, size_t keylen,
                       unsigned char *value, size_t vallen);

typedef int foreach_cb(void *data,
                       unsigned char *key, size_t keylen,
                       unsigned char *value, size_t vallen);

/*
 * Zeroskip API
 */
extern int zsdb_init(struct zsdb **pdb);
extern void zsdb_final(struct zsdb **pdb);
extern int zsdb_open(struct zsdb *db, const char *dbdir, int mode);
extern int zsdb_close(struct zsdb *db);
extern int zsdb_add(struct zsdb *db, unsigned char *key, size_t keylen,
                    unsigned char *value, size_t vallen);
extern int zsdb_remove(struct zsdb *db, unsigned char *key, size_t keylen);
extern int zsdb_commit(struct zsdb *db);
extern int zsdb_fetch(struct zsdb *db, unsigned char *key, size_t keylen,
                      unsigned char **value, size_t *vallen, struct txn **txn);
extern int zsdb_foreach(struct zsdb *db, const char *prefix, size_t prefixlen,
                        foreach_p *p, foreach_cb *cb, void *cbdata,
                        struct txn **txn);
extern int zsdb_forone(struct zsdb *db, unsigned char *key, size_t keylen,
                       foreach_p *p, foreach_cb *cb, void *cbdata,
                       struct txn **txn);
extern int zsdb_dump(struct zsdb *db, DBDumpLevel level);
extern int zsdb_abort(struct zsdb *db);
extern int zsdb_consistent(struct zsdb *db);
extern int zsdb_repack(struct zsdb *db);
extern int zsdb_info(struct zsdb *db);
extern int zsdb_finalise(struct zsdb *db);

extern int zsdb_transaction_begin(struct zsdb *db, struct txn **txn);
extern void zsdb_transaction_end(struct txn **txn);

/* locking routines */
extern int zsdb_write_lock_acquire(struct zsdb *db, long timeout_ms);
extern int zsdb_write_lock_release(struct zsdb *db);
extern int zsdb_write_lock_is_locked(struct zsdb *db);
extern int zsdb_pack_lock_acquire(struct zsdb *db, long timeout_ms);
extern int zsdb_pack_lock_release(struct zsdb *db);
extern int zsdb_pack_lock_is_locked(struct zsdb *db);

CPP_GUARD_END
#endif  /* _ZEROSKIP_H_ */
