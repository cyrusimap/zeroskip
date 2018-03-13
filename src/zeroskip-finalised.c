/*
 * zeroskip
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */
#include "log.h"
#include "util.h"
#include "zeroskip.h"
#include "zeroskip-priv.h"

/*
 * Public functions
 */
int zs_finalised_file_open(const char *path, struct zsdb_file **fptr)
{
        int ret = ZS_OK;
        struct zsdb_file *f;
        size_t mf_size;
        int mappedfile_flags = MAPPEDFILE_RD;

        f = xcalloc(sizeof(struct zsdb_file), 1);
        f->type = DB_FTYPE_FINALISED;
        cstring_init(&f->fname, 0);
        cstring_addstr(&f->fname, path);

      /* Open the filename for use */
        ret = mappedfile_open(f->fname.buf,
                              mappedfile_flags, &f->mf);
        if (ret) {
                zslog(LOGDEBUG, "Could not open %s in read-only mode.\n",
                        f->fname.buf);
                ret = ZS_IOERROR;
                goto done;
        }

        f->is_open = 1;

        /* XXX: No need to check for size here, since zs_header_valid()
           will do it */
        mappedfile_size(&f->mf, &mf_size);
        if (mf_size <= ZS_HDR_SIZE) {
                ret = ZS_INVALID_FILE;
                zslog(LOGDEBUG, "%s is not a valid finalised file.\n",
                        f->fname.buf);
                goto fail;
        }

        if (zs_header_validate(f)) {
                zslog(LOGDEBUG, "Could not valid header in %s.\n",
                        f->fname.buf);
                ret = ZS_INVALID_DB;
                goto fail;
        }

       /* Seek to location after header */
        mf_size = ZS_HDR_SIZE;
        mappedfile_seek(&f->mf, mf_size, NULL);

        *fptr = f;

        goto done;

fail:                           /* Jump here on failure */
        mappedfile_close(&f->mf);
        cstring_release(&f->fname);
        xfree(f);

done:
        return ret;
}

int zs_finalised_file_close(struct zsdb_file **fptr)
{
        int ret = ZS_OK;
        struct zsdb_file *f;

        if (fptr == NULL || *fptr == NULL)
                return ret;

        f = *fptr;
        fptr = NULL;

        mappedfile_close(&f->mf);
        cstring_release(&f->fname);
        xfree(f);

        return ret;
}

int zs_finalised_file_record_foreach(struct zsdb_file *f,
                                     foreach_cb *cb, void *cbdata)
{
        int ret = ZS_OK;
        size_t mfsize = 0, offset = ZS_HDR_SIZE;

        mappedfile_size(&f->mf, &mfsize);
        if (mfsize == 0 || mfsize < ZS_HDR_SIZE) {
                zslog(LOGDEBUG, "Not a valid finalised DB file.\n");
                return ZS_INVALID_DB;
        } else if (mfsize == ZS_HDR_SIZE) {
                zslog(LOGDEBUG, "No records in finalised DB file.\n");
                return ret;
        }

        while (offset < mfsize) {
                ret = zs_record_read_from_file(f, &offset,
                                               cb, cbdata);
        }

        return ret;
}
