/*
 * zeroskip-active.c
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 */

#include "log.h"
#include "zeroskip.h"
#include "zeroskip-priv.h"

int zs_active_file_open(struct zsdb_priv *priv, uint32_t idx)
{
        int ret = ZS_OK;
        size_t mf_size;
        int mappedfile_flags = MAPPEDFILE_RW | MAPPEDFILE_CREATE;

        zs_filename_generate_active(priv, &priv->factive.fname);

        /* Initialise the header fields */
        priv->factive.header.signature = ZS_SIGNATURE;
        priv->factive.header.version = ZS_VERSION;
        priv->factive.header.startidx = idx;
        priv->factive.header.endidx = idx;
        priv->factive.header.crc32 = 0;

        /* Open the active filename for use */
        ret = mappedfile_open(priv->factive.fname.buf,
                              mappedfile_flags, &priv->factive.mf);
        if (ret) {
                ret = ZS_IOERROR;
                goto done;
        }

        priv->factive.is_open = 1;

        mappedfile_size(&priv->factive.mf, &mf_size);
        /* The filesize is zero, it is a new file. */
        if (mf_size == 0) {
                ret = zs_header_write(&priv->factive);
                if (ret) {
                        fprintf(stderr, "Could not write zeroskip header.\n");
                        mappedfile_close(&priv->factive.mf);
                        goto done;
                }
        }

        if (zs_header_validate(&priv->factive)) {
                ret = ZS_INVALID_DB;
                mappedfile_close(&priv->factive.mf);
                goto done;
        }

        /* Seek to location after header */
        mf_size = ZS_HDR_SIZE;
        mappedfile_seek(&priv->factive.mf, mf_size, NULL);
done:
        return ret;
}

int zs_active_file_close(struct zsdb_priv *priv)
{
        int ret = ZS_OK;

        if (!priv->factive.is_open)
                return ZS_ERROR;

        /* TODO: Check if we are in the middle of a transaction,
         *       If we are, then commit before closing.
         */
        mappedfile_flush(&priv->factive.mf);
        mappedfile_close(&priv->factive.mf);

        cstring_release(&priv->factive.fname);

        return ret;
}

int zs_active_file_finalise(struct zsdb_priv *priv _unused_)
{
        int ret = ZS_OK;

        return ret;
}
