/*
 * zeroskip
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#include "cstring.h"
#include "log.h"
#include "macros.h"
#include "util.h"
#include "zeroskip.h"
#include "zeroskip-priv.h"

#include <errno.h>
#include <fts.h>
#include <libgen.h>
#include <stdlib.h>
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <zlib.h>

/**
 * Private functions
 */
static int process_active_file(const char *path, void *data _unused_)
{
        zslog(LOGDEBUG, "processing active file: %s\n", path);
        return 0;
}
static int process_finalised_file(const char *path, void *data _unused_)
{
        zslog(LOGDEBUG, "processing finalised file: %s\n", path);
        return 0;
}
static int process_packed_file(const char *path, void *data _unused_)
{
        zslog(LOGDEBUG, "processing packed file: %s\n", path);
        return 0;
}

static enum db_ftype_t interpret_db_filename(const char *str, size_t len)
{
        const char *p;
        const char *idx;
        uint32_t startidx = 0, endidx = 0;
        enum db_ftype_t type = DB_FTYPE_UNKNOWN;

        p = memmem(str, len, ZS_FNAME_PREFIX, ZS_FNAME_PREFIX_LEN);
        if (!p)
                goto done;

        idx = p + ZS_FNAME_PREFIX_LEN + (UUID_STRLEN - 1);

        /* We should have atleast 1 index or a max of 2 */
        if (*idx++ == '-') {
                startidx = strtoul(idx, (char **)&idx, 10);
                type = DB_FTYPE_ACTIVE;
        }

        if (*idx && *idx++ == '-') {
                endidx = strtoul(idx, (char **)&idx, 10);

                type = (endidx == startidx) ?
                        DB_FTYPE_FINALISED : DB_FTYPE_PACKED;
        }

done:
        return type;
}

enum {
        DB_REL_PATH = 0,
        DB_ABS_PATH = 1,
};

static int for_each_db_file_in_dbdir(char *const path[],
                                     int full_path,
                                     void *data)
{
        FTS *ftsp = NULL;
        FTSENT *fp = NULL;
        int fts_options = FTS_NOCHDIR;
        char *const def_path[] = {".", NULL};
        char buf[PATH_MAX];
        int err = 0;

        if (getcwd(buf, sizeof(buf)) == NULL)
                return errno;

        ftsp = fts_open(*path ? path : def_path, fts_options, NULL);
        if (ftsp == NULL) {
                perror("fts_open:");
                return errno;
        }

        while ((fp = fts_read(ftsp)) != NULL) {
                char *bname;
                char sbuf[PATH_MAX];

                if (fp->fts_info == FTS_DNR ||
                    fp->fts_info == FTS_ERR ||
                    fp->fts_info == FTS_NS) {
                        err = fp->fts_errno;
                        break;
                }

                if (fp->fts_info != FTS_F)
                        continue;

                bname = basename(fp->fts_path);

                if (full_path)
                        snprintf(sbuf, PATH_MAX, "%s/%s/%s", buf,
                                 *path ? *path : buf, bname);
                else
                        snprintf(sbuf, PATH_MAX, "%s/%s", *path ? *path : buf, bname);

                if (strncmp(bname, ZS_FNAME_PREFIX, ZS_FNAME_PREFIX_LEN) == 0) {
                        switch(interpret_db_filename(sbuf, strlen(sbuf))) {
                        case DB_FTYPE_ACTIVE:
                                process_active_file(sbuf, data);
                                break;
                        case DB_FTYPE_FINALISED:
                                process_finalised_file(sbuf, data);
                                break;
                        case DB_FTYPE_PACKED:
                                process_packed_file(sbuf, data);
                                break;
                        default:
                                break;
                        } /* switch() */
                }         /* strncmp() */
        }                 /* fts_read() */

        fts_close(ftsp);

        if (err)
                errno = err;

        return err;
}


/**
 * Public functions
 */
#ifdef ZS_DEBUG
void assert_zsdb(struct zsdb *db)
{
        assert(db);
        assert(db->priv);
}

int zsdb_break(int err)
{
        return err;
}
#else
#define assert_zsdb(x)
#endif

int zsdb_init(struct zsdb **pdb)
{
        struct zsdb *db;
        struct zsdb_priv *priv;
        int ret = ZS_OK;

        db = xcalloc(1, sizeof(struct zsdb));
        if (!db) {
                *pdb = NULL;
                ret = ZS_NOMEM;
                goto done;
        }

        priv = xcalloc(1, sizeof(struct zsdb_priv));
        if (!priv) {
                xfree(db);
                ret = ZS_NOMEM;
                goto done;
        }
        db->priv = priv;

        *pdb = db;

done:
        return ret;
}

void zsdb_final(struct zsdb **pdb)
{
        struct zsdb *db;
        struct zsdb_priv *priv;

        if (pdb && *pdb) {
                db = *pdb;
                priv = db->priv;

                cstring_release(&priv->dbdir);

                xfree(priv);
                xfree(db);
        }
}

static int zsdb_create(struct zsdb *db)
{
        struct zsdb_priv *priv = db->priv;
        struct stat sb = { 0 };
        mode_t mode = 0777;

        zslog(LOGDEBUG, "Creating a new DB %s!\n", priv->dbdir.buf);
        /* Create the dbdir */
        if (xmkdir(priv->dbdir.buf, mode) != 0) {
                perror("zs_init:");
                return ZS_ERROR;
        }

        /* Stat again to make sure that the directory got created */
        if (stat(priv->dbdir.buf, &sb) == -1) {
                /* If the directory isn't created, we have serious issues
                 * with the underlying disk.
                 */
                zslog(LOGWARNING, "Could not create DB %s\n",
                        priv->dbdir.buf);
                return ZS_ERROR; /* Abort? */
        }

        /* Create the .zsdb file */
        if (!zs_dotzsdb_create(priv)) {
                zslog(LOGWARNING, "Failed creating DB metadata.\n");
                return ZS_ERROR;
        }

        return ZS_OK;
}

int zsdb_open(struct zsdb *db, const char *dbdir, int flags)
{
        struct zsdb_priv *priv;
        int ret = ZS_OK;
        struct stat sb = { 0 };

        assert_zsdb(db);
        assert(dbdir && dbdir[0]);

        if (!db || !db->priv) {
                zslog(LOGDEBUG, "db not initilaized.\n");
                return ZS_ERROR;
        }

        priv = db->priv;

        if (priv->nopen && (flags & OWRITE)) {
                zslog(LOGWARNING, "db can only be opened in read mode\n");
                return ZS_INVALID_MODE;
        }

        cstring_release(&priv->dbdir);
        cstring_addstr(&priv->dbdir, dbdir);

        /* stat() the dbdir */
        if (stat(priv->dbdir.buf, &sb) == -1) {
                zslog(LOGDEBUG, "DB %s doesn't exist\n",
                        priv->dbdir.buf);
                if (flags & OCREAT) {
                        ret = zsdb_create(db);
                } else {
                        ret = ZS_ERROR;
                        goto done;
                }
        } else {
                if (!S_ISDIR(sb.st_mode) || !zs_dotzsdb_validate(priv)) {
                        zslog(LOGWARNING, "%s, isn't a valid DB\n", priv->dbdir.buf);
                        ret = ZS_ERROR;
                        goto done;
                }
        }

        /* scan the directory for files */
        for_each_db_file_in_dbdir(&priv->dbdir.buf, DB_ABS_PATH, priv);

        if (flags & OWRITE) {
                zslog(LOGDEBUG, "Opening DB in WRITE mode.\n");
        } else if (flags & OREAD) {
                zslog(LOGDEBUG, "Opening DB in READ mode.\n");
        }


done:
        return ret;

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
        return ZS_NOTIMPLEMENTED;
}

int zsdb_remove(struct zsdb *db, unsigned char *key, size_t keylen)
{
        return ZS_NOTIMPLEMENTED;
}

int zsdb_fetch(struct zsdb *db, unsigned char *key, size_t keylen,
               unsigned char **value, size_t *vallen)
{
        return ZS_NOTIMPLEMENTED;
}
int zsdb_dump(struct zsdb *db, DBDumpLevel level)
{
        return ZS_NOTIMPLEMENTED;
}
