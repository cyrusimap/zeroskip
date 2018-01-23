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
};

/* structs */
struct zsdb_ops;
struct zsdb_iter;
struct zsdb;


extern int zsdb_init(struct zsdb **pdb);
extern int zsdb_open(struct zsdb *db, const char *dbdir);
extern int zsdb_close(struct zsdb *db);
extern int zsdb_add(struct zsdb *db, unsigned char *key, size_t keylen,
                    unsigned char *value, unsigned char *vallen);
extern int zsdb_remove(struct zsdb *db, unsigned char *key, size_t keylen);
extern int zsdb_dump(struct zsdb *db, DBDumpLevel level);

CPP_GUARD_END
#endif  /* _ZEROSKIP_H_ */
