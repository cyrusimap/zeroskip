/*
 * zeroskip
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */
#include "btree.h"
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
static int load_active_records_cb(void *data,
                                  unsigned char *key, size_t keylen,
                                  unsigned char *value, size_t vallen)
{
        struct btree *memtree = (struct btree *)data;
        struct record *rec;

        btree_remove(memtree, key, keylen);

        rec = record_new(key, keylen, value, vallen);
        btree_insert(memtree, rec);

        return 0;
}

static int process_active_file(const char *path, void *data)
{
        struct zsdb_priv *priv;
        int ret = ZS_OK;
        size_t mfsize;

        if (!data) {
                zslog(LOGDEBUG, "Internal error when preocessing active file.\n");
                ret = ZS_INTERNAL;
                goto done;
        }

        priv = (struct zsdb_priv *)data;

        if (priv->factive.is_open) {
                zslog(LOGWARNING, "DB has more than one active file. Invalid!\n");
                ret = ZS_INTERNAL;
                goto done;
        }

        if ((ret = zs_active_file_open(priv, 0, 0)) != ZS_OK) {
                file_lock_release(&priv->wlk);
                goto done;
        }

        /* Seek to the end of the file, that's where the
           records need to appended to.
        */
        mappedfile_size(&priv->factive.mf, &mfsize);
        if (mfsize < ZS_HDR_SIZE) {
                zslog(LOGWARNING, "DB %s is in an invalid state.\n",
                        priv->factive.fname.buf);
                ret = ZS_INVALID_DB;
                goto done;
        }

        /* Load records from active file to in-memory tree */
        zs_active_file_record_foreach(priv, load_active_records_cb,
                                      priv->memtree);

        if (mfsize)
                mappedfile_seek(&priv->factive.mf, mfsize, NULL);

        zslog(LOGDEBUG, "opened active file: %s\n", path);
done:
        return ret;
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
                                     void *data,
                                     int *count)
{
        FTS *ftsp = NULL;
        FTSENT *fp = NULL;
        int fts_options = FTS_NOCHDIR;
        char *const def_path[] = {".", NULL};
        char buf[PATH_MAX];
        int err = 0;
        int cnt = 0;
        int ret = ZS_OK;

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
                                /* XXX: Shouldn't have more than one active file */
                                ret = process_active_file(sbuf, data);
                                cnt++;
                                break;
                        case DB_FTYPE_FINALISED:
                                ret = process_finalised_file(sbuf, data);
                                cnt++;
                                break;
                        case DB_FTYPE_PACKED:
                                ret = process_packed_file(sbuf, data);
                                cnt++;
                                break;
                        default:
                                break;
                        } /* switch() */
                }         /* strncmp() */
                if (ret != ZS_OK) {
                        zslog(LOGDEBUG, "Failed processing DB %s\n", *path);
                        break;
                }
        }                 /* fts_read() */

        fts_close(ftsp);

        if (err)
                errno = err;

        err = ret;

        *count = cnt;
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

int zsdb_open(struct zsdb *db, const char *dbdir, int mode)
{
        struct zsdb_priv *priv;
        int ret = ZS_OK;
        struct stat sb = { 0 };
        int fcount = 0;
        int newdb = 0;

        assert_zsdb(db);
        assert(dbdir && dbdir[0]);

        if (!db || !db->priv) {
                zslog(LOGDEBUG, "db not initilaized.\n");
                return ZS_ERROR;
        }

        priv = db->priv;

        cstring_release(&priv->dbdir);
        cstring_addstr(&priv->dbdir, dbdir);

        /* db file list */
        priv->dbflist.prev = &priv->dbflist;
        priv->dbflist.next = &priv->dbflist;

        /* stat() the dbdir */
        if (stat(priv->dbdir.buf, &sb) == -1) {
                zslog(LOGDEBUG, "DB %s doesn't exist\n",
                        priv->dbdir.buf);
                if (mode == MODE_CREATE) {
                        ret = zsdb_create(db);
                        newdb = 1;
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

        /* We either should have created a new DB directory or should
         * have successfully opened an existing DB directory.
         */
        if (ret != ZS_OK) {
                zslog(LOGWARNING, "Internal Error!\n");
                goto done;
        }

        /* In-memory tree */
        priv->memtree = btree_new(NULL, NULL);

        if (newdb) {
                if (zsdb_write_lock_acquire(db, 0 /*timeout*/) < 0) {
                        zslog(LOGDEBUG, "Cannot acquire a write lock on %s\n",
                              priv->dbdir.buf);
                        ret = ZS_ERROR;
                        goto done;
                }

                /* Create the 'active' mutable db file if it is
                 * a newly created DB.
                 */
                ret = zs_active_file_open(priv, 0, 1);

                zsdb_write_lock_release(db);
                if (ret != ZS_OK)
                        goto done;

                zslog(LOGDEBUG, "Created active file: %s\n",
                      priv->factive.fname.buf);
        } else {
                /* If it is an existing DB, scan the directory for
                 * db files.
                 */
                ret = for_each_db_file_in_dbdir(&priv->dbdir.buf,
                                                DB_ABS_PATH, priv,
                                                &fcount);
                if (ret != ZS_OK)
                        goto done;

                zslog(LOGDEBUG, "Found %d files in %s.\n",
                      fcount, priv->dbdir.buf);
        }

        zslog(LOGDEBUG, "DB `%s` opened.\n", priv->dbdir.buf);

        priv->open = 1;

done:
        return ret;
}

int zsdb_close(struct zsdb *db)
{
        int ret  = ZS_OK;
        struct zsdb_priv *priv;

        if (!db) {
                ret = ZS_IOERROR;
                goto done;
        }

        priv = db->priv;

        zslog(LOGDEBUG, "Closing DB `%s`.\n", priv->dbdir.buf);

        if (priv->factive.is_open)
                zsdb_write_lock_release(db);

        zs_active_file_close(priv);

        if (priv->memtree)
                btree_free(priv->memtree);

        if (db->iter || db->numtrans)
                ret = zsdb_break(ZS_INTERNAL);
done:
        return ret;
}

int zsdb_add(struct zsdb *db,
             unsigned char *key,
             size_t keylen,
             unsigned char *value,
             size_t vallen)
{
        int ret = ZS_OK;
        struct zsdb_priv *priv;
        size_t mfsize;
        struct record *rec;

        assert_zsdb(db);
        assert(key);
        assert(keylen);
        assert(value);
        assert(vallen);

        if (!db)
                return ZS_NOT_OPEN;

        if (!key || !value)
                return ZS_ERROR;

        priv = db->priv;

        if (!priv->open || !priv->factive.is_open) {
                return ZS_NOT_OPEN;
        }

        if (!zsdb_write_lock_is_locked(db)) {
                zslog(LOGDEBUG, "Need a write lock to add records.\n");
                ret = ZS_ERROR;
                goto done;
        }

        /* check file size and finalise if necessary */
        mappedfile_size(&priv->factive.mf, &mfsize);
        if (mfsize >= TWOMB) {
                zslog(LOGDEBUG, "File %s is > 2MB, finalising.\n",
                        priv->factive.fname.buf);
                ret = zs_active_file_finalise(priv);
                if (ret != ZS_OK) goto done;

                ret = zs_active_file_new(priv,
                                         priv->dotzsdb.curidx + 1);
                zslog(LOGDEBUG, "New active log file %s created.\n",
                        priv->factive.fname.buf);
        }

        /* Start computing the crc32. Will end when the transaction is
           committed */
        crc32_begin(&priv->factive.mf);

        /* Add the entry to the active file */
        ret = zs_active_file_write_keyval_record(priv, key, keylen, value,
                                                 vallen);
        if (ret != ZS_OK) {
                crc32_end(&priv->factive.mf);
                goto done;
        }
        priv->factive.dirty = 1;

        /* Add the entry to the in-memory tree */
        rec = record_new(key, keylen, value, vallen);
        btree_remove(priv->memtree, key, keylen);
        btree_insert(priv->memtree, rec);

        /* TODO: REMOVE THE PRINTING! */
        /* btree_print_node_data(priv->memtree, NULL); */

        zslog(LOGDEBUG, "Inserted record into the DB.\n");

done:
        return ret;
}

int zsdb_remove(struct zsdb *db,
                unsigned char *key,
                size_t keylen)
{
        int ret = ZS_OK;
        struct zsdb_priv *priv;
        struct record *rec;

        assert_zsdb(db);
        assert(key);
        assert(keylen);

        if (!db || !db->priv) {
                ret = ZS_NOT_OPEN;
                goto done;
        }

        priv = db->priv;

        if (!priv->open || !priv->factive.is_open) {
                return ZS_NOT_OPEN;
        }

        if (!zsdb_write_lock_is_locked(db)) {
                zslog(LOGDEBUG, "Need a write lock to remove records.\n");
                ret = ZS_ERROR;
                goto done;
        }

        /* Start computing the crc32. Will end when the transaction is
           committed */
        crc32_begin(&priv->factive.mf);

        ret = zs_active_file_write_delete_record(priv, key, keylen);
        if (ret != ZS_OK) {
                crc32_end(&priv->factive.mf);
                zslog(LOGDEBUG, "Failed removing key from DB `%s`\n",
                      priv->dbdir.buf);
                goto done;
        }
        priv->factive.dirty = 1;

        zslog(LOGDEBUG, "Removed key from DB `%s`\n", priv->dbdir.buf);

        /* Add the entry to the in-memory tree */
        rec = record_new(key, keylen, NULL, 0);
        btree_remove(priv->memtree, key, keylen);
        btree_insert(priv->memtree, rec);

        /* TODO: REMOVE THE PRINTING! */
        /* btree_print_node_data(priv->memtree, NULL); */

done:
        return ret;
}

int zsdb_commit(struct zsdb *db)
{
        int ret = ZS_OK;
        struct zsdb_priv *priv;

        assert(db);
        assert(db->priv);

        priv = db->priv;

        if (!priv->factive.is_open)
                return ZS_NOT_OPEN;

        ret = zs_active_file_write_commit_record(priv);
        if (ret == ZS_OK)
                priv->factive.dirty = 0;

        return ret;
}

int zsdb_fetch(struct zsdb *db _unused_,
               unsigned char *key _unused_,
               size_t keylen _unused_,
               unsigned char **value _unused_,
               size_t *vallen _unused_)
{
        return ZS_NOTIMPLEMENTED;
}

static int print_rec(void *data _unused_,
                     unsigned char *key, size_t keylen,
                     unsigned char *val, size_t vallen)
{
        size_t i;

        for (i = 0; i < keylen; i++) {
                printf("%c", key[i]);
        }
        if (keylen) printf("\n");

        for (i = 0; i < vallen; i++) {
                printf("%c", val[i]);
        }
        if (vallen) printf("\n");

        printf("---\n");

        return 0;
}

int zsdb_dump(struct zsdb *db,
              DBDumpLevel level)
{
        int ret = ZS_OK;
        struct zsdb_priv *priv;

        assert_zsdb(db);

        if (db)
                priv = db->priv;

        if (!priv->open) {
                zslog(LOGWARNING, "DB `%s` not open.\n!", priv->dbdir.buf);
                return ZS_NOT_OPEN;
        }

        if (level == DB_DUMP_ACTIVE) {
                ret = zs_active_file_record_foreach(priv, print_rec,
                                                    NULL);
        } else if (level == DB_DUMP_ALL) {
        } else {
                zslog(LOGDEBUG, "Invalid DB dump option\n");
                return ZS_ERROR;
        }

        return ret;
}

int zsdb_abort(struct zsdb *db)
{
        int ret = ZS_OK;

        assert_zsdb(db);

        return ret;
}

int zsdb_consistent(struct zsdb *db)
{
        int ret = ZS_OK;

        assert_zsdb(db);

        return ret;
}

int zsdb_repack(struct zsdb *db)
{
        int ret = ZS_OK;

        assert_zsdb(db);

        return ret;
}

int zsdb_info(struct zsdb *db)
{
        int ret = ZS_OK;

        assert_zsdb(db);

        return ret;
}

#define WRITE_LOCK_FNAME "zsdbw"
#define PACK_LOCK_FNAME "zsdbp"

int zsdb_write_lock_acquire(struct zsdb *db, long timeout_ms)
{
        struct zsdb_priv *priv;
        int ret;

        assert(db);

        priv = db->priv;
        if (!priv) return ZS_INTERNAL;
        ret = file_lock_acquire(&priv->wlk, priv->dbdir.buf,
                                WRITE_LOCK_FNAME, timeout_ms);

        return ret ? ZS_OK : ZS_ERROR;
}

int zsdb_write_lock_release(struct zsdb *db)
{
        struct zsdb_priv *priv;

        assert(db);

        priv = db->priv;
        if (!priv) return ZS_INTERNAL;
        return (file_lock_release(&priv->wlk)  == 0) ? ZS_OK : ZS_ERROR;
}

int zsdb_write_lock_is_locked(struct zsdb *db)
{
        struct zsdb_priv *priv;

        assert(db);

        priv = db->priv;
        if (!priv) return ZS_INTERNAL;
        return file_lock_is_locked(&priv->wlk);
}

int zsdb_pack_lock_acquire(struct zsdb *db, long timeout_ms)
{
        struct zsdb_priv *priv;

        assert(db);

        priv = db->priv;
        if (!priv) return ZS_INTERNAL;
        return file_lock_acquire(&priv->plk, priv->dbdir.buf,
                                 PACK_LOCK_FNAME, timeout_ms);
}

int zsdb_pack_lock_release(struct zsdb *db)
{
        struct zsdb_priv *priv;

        assert(db);

        priv = db->priv;
        if (!priv) return ZS_INTERNAL;

        return file_lock_release(&priv->plk);
}

int zsdb_pack_lock_is_locked(struct zsdb *db)
{
        struct zsdb_priv *priv;

        assert(db);

        priv = db->priv;
        if (!priv) return ZS_INTERNAL;
        return file_lock_is_locked(&priv->plk);
}
