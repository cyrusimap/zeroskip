/*
 * zeroskip-filename.c
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#include "log.h"
#include "zeroskip.h"
#include "zeroskip-priv.h"

/* zs_filename_generate_active():
 * Generates a new filename for the active file
 */
void zs_filename_generate_active(struct zsdb_priv *priv, cstring *fname)
{
        char index[11];

        memset(index, 0, sizeof(index));
        snprintf(index, 20, "%d", priv->dotzsdb.curidx);

        cstring_release(fname);

        cstring_dup(&priv->dbdir, fname);
        cstring_addch(fname, '/');
        cstring_addstr(fname, ZS_FNAME_PREFIX);
        cstring_add(fname, priv->dotzsdb.uuidstr, UUID_STRLEN - 1);
        cstring_addch(fname, '-');
        cstring_addstr(fname, index);
}

/* zs_filename_generate_packed():
 * Generates a new filename for a packed file
 */
void zs_filename_generate_packed(struct zsdb_priv *priv, cstring *fname,
                                 uint32_t startidx, uint32_t endidx)
{
        char sidx[11];
        char eidx[11];

        memset(sidx, 0, sizeof(sidx));
        memset(eidx, 0, sizeof(eidx));

        snprintf(sidx, 20, "%d", startidx);
        snprintf(eidx, 20, "%d", endidx);

        cstring_release(fname);

        cstring_dup(&priv->dbdir, fname);
        cstring_addch(fname, '/');
        cstring_addstr(fname, ZS_FNAME_PREFIX);
        cstring_add(fname, priv->dotzsdb.uuidstr, UUID_STRLEN - 1);
        cstring_addch(fname, '-');
        cstring_addstr(fname, sidx);
        cstring_addch(fname, '-');
        cstring_addstr(fname, eidx);

}
