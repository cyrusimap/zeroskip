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

CPP_GUARD_START

typedef enum {
        DB_DUMP_ACTIVE,
        DB_DUMP_ALL,
} DBDumpLevel;

/* Return codes */
enum {
        SDB_OK             =  0,
        SDB_DONE           =  1,
        SDB_IOERROR        = -1,
        SDB_AGAIN          = -2,
        SDB_EXISTS         = -3,
        SDB_INTERNAL       = -4,
        SDB_NOTFOUND       = -5,
        SDB_NOTIMPLEMENTED = -6,
        SDB_FULL           = -7,
        SDB_ERROR          = -8,
        SDB_INVALID_DB     = -9,
};

CPP_GUARD_END

#endif  /* _ZEROSKIP_H_ */
