/*
 * zeroskip
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#include "util.h"
#include "zeroskip.h"
#include "zeroskip-priv.h"

#ifdef ZS_DEBUG
int zsdb_break(int err)
{
        return err;
}
#endif

/**
 * Public functions
 */
int zsdb_init(struct zsdb **pdb)
{
        struct zsdb *db;
        int ret = ZS_OK;

        db = xcalloc(1, sizeof(struct zsdb));
        if (!db) {
                *pdb = NULL;
                ret = ZS_NOMEM;
                goto done;
        }

        *pdb = db;

done:
        return ZS_OK;
}

int zsdb_open(struct zsdb *db, const char *dbdir)
{
        
}

int zsdb_close(struct zsdb *db)
{
        int ret  = ZS_OK;

        if (!db) {
                ret = ZS_IOERROR;
                goto done;
        }

        if (db->iter || db->numtrans)
                ret = zsdb_break(ZS_INTERNAL);
        else {
                xfree(db);
        }

done:
        return ret;
}

int zsdb_add(struct zsdb *db, unsigned char *key, size_t keylen,
                    unsigned char *value, unsigned char *vallen)
{
}

int zsdb_remove(struct zsdb *db, unsigned char *key, size_t keylen)
{
}

int zsdb_dump(struct zsdb *db, DBDumpLevel level)
{
}
