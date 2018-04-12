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
#include "pqueue.h"
#include "util.h"
#include "zeroskip.h"
#include "zeroskip-priv.h"

#include <errno.h>

#if defined(LINUX) || defined(DARWIN) || defined(BSD)
#include <fts.h>
#else
#include <dirent.h>
#endif

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

/* PQ for storing packed and finalised file names temporarily when
 * opening the DB
 */
static struct pqueue finalisedpq;
static struct pqueue packedpq;

/**
 * Private functions
 */
static int dbfname_cmp(const void *d1, const void *d2, void *cbdata _unused_)
{
        struct zsdb_file *f1 = (struct zsdb_file *)d1;
        struct zsdb_file *f2 = (struct zsdb_file *)d2;

        return natural_strcasecmp(f1->fname.buf, f2->fname.buf);
}

static int load_records_cb(void *data,
                           unsigned char *key, size_t keylen,
                           unsigned char *value, size_t vallen)
{
        struct btree *memtree = (struct btree *)data;
        struct record *rec;

        rec = record_new(key, keylen, value, vallen);
        btree_replace(memtree, rec);

        return 0;
}

static int print_record_cb(void *data _unused_,
                           unsigned char *key, size_t keylen,
                           unsigned char *value, size_t vallen)
{
        size_t i;

        for (i = 0; i < keylen; i++) {
                printf("%c", key[i]);
        }
        if (keylen) printf("\n");

        for (i = 0; i < vallen; i++) {
                printf("%c", value[i]);
        }
        if (vallen) printf("\n");

        printf("---\n");

        return 0;
}

static int process_active_file(const char *path, void *data)
{
        struct zsdb_priv *priv;
        int ret = ZS_OK;

        if (!data) {
                zslog(LOGDEBUG, "Internal error when preocessing active file.\n");
                ret = ZS_INTERNAL;
                goto done;
        }

        priv = (struct zsdb_priv *)data;

        if (priv->dbfiles.factive.is_open || (priv->dbfiles.afcount == 1)) {
                zslog(LOGWARNING, "DB has more than one active file. Invalid!\n");
                ret = ZS_INTERNAL;
                goto done;
        }

        if ((ret = zs_active_file_open(priv, 0, 0)) != ZS_OK) {
                file_lock_release(&priv->wlk);
                goto done;
        }

        priv->dbfiles.afcount++;
        zslog(LOGDEBUG, "opened active file: %s\n", path);
done:
        return ret;
}

static int process_finalised_file(const char *path, void *data _unused_)
{
        int ret = ZS_OK;
        struct zsdb_file *f = NULL;

        if (!data) {
                zslog(LOGDEBUG, "Internal error when preocessing active file.\n");
                ret = ZS_INTERNAL;
                goto done;
        }

        zslog(LOGDEBUG, "processing finalised file: %s\n", path);

        ret = zs_finalised_file_open(path, &f);
        if (ret != ZS_OK) {
                zslog(LOGDEBUG, "skipping file %s\n", path);
                goto done;
        }

        pqueue_put(&finalisedpq, f);

done:
        return ret;
}

static int process_packed_file(const char *path, void *data)
{
        int ret = ZS_OK;
        struct zsdb_file *f _unused_;

        if (!data) {
                zslog(LOGDEBUG, "Internal error when preocessing active file.\n");
                ret = ZS_INTERNAL;
                goto done;
        }

        zslog(LOGDEBUG, "processing packed file: %s\n", path);

        ret = zs_packed_file_open(path, &f);
        if (ret != ZS_OK) {
                zslog(LOGDEBUG, "skipping file %s\n", path);
                goto done;
        }

        pqueue_put(&packedpq, f);

done:
        return ret;
}

static enum db_ftype_t interpret_db_filename(const char *str, size_t len,
                                             uint32_t *sidx, uint32_t *eidx)
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
                if (sidx) *sidx = startidx;
                type = DB_FTYPE_ACTIVE;
        }

        if (*idx && *idx++ == '-') {
                endidx = strtoul(idx, (char **)&idx, 10);
                if (eidx) *eidx = endidx;

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

#if defined(LINUX) || defined(DARWIN) || defined(BSD)
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
        int ret = ZS_OK;

        if (getcwd(buf, sizeof(buf)) == NULL)
                return errno;

        ftsp = fts_open(*path ? path : def_path, fts_options, NULL);
        if (ftsp == NULL) {
                err = errno;
                perror("fts_open:");
                errno = err;
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
                        switch(interpret_db_filename(sbuf, strlen(sbuf),
                                                     NULL, NULL)) {
                        case DB_FTYPE_ACTIVE:
                                /* XXX: Shouldn't have more than one active file */
                                ret = process_active_file(sbuf, data);
                                break;
                        case DB_FTYPE_FINALISED:
                                ret = process_finalised_file(sbuf, data);
                                break;
                        case DB_FTYPE_PACKED:
                                ret = process_packed_file(sbuf, data);
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

        return err;
}
#else  /* SOLARIS */
static int for_each_db_file_in_dbdir(char *const path[],
                                     int full_path,
                                     void *data)
{
        char buf[PATH_MAX];
        int err = 0;
        int ret = ZS_OK;
        DIR *dir;
        struct dirent *de;

        if (getcwd(buf, sizeof(buf)) == NULL)
                return errno;

        dir = opendir(*path ? *path : ".");
        if (!dir)
                return ZS_ERROR;

        while ((de = readdir(dir)) != NULL) {
                char *bname;
                char sbuf[PATH_MAX];
                struct stat sb;

                if (is_dotdir(de->d_name))
                        continue;

                bname = basename(de->d_name);

                if (full_path)
                        snprintf(sbuf, PATH_MAX, "%s/%s/%s", buf,
                                 *path ? *path : buf, bname);
                else
                        snprintf(sbuf, PATH_MAX, "%s/%s", *path ? *path : buf, bname);

                if (lstat(sbuf, &sb) != 0) {
                        continue;
                }

                if (S_ISDIR(sb.st_mode))
                        continue;

                if (strncmp(bname, ZS_FNAME_PREFIX, ZS_FNAME_PREFIX_LEN) == 0) {
                        switch(interpret_db_filename(sbuf, strlen(sbuf),
                                                     NULL, NULL)) {
                        case DB_FTYPE_ACTIVE:
                                /* XXX: Shouldn't have more than one active file */
                                ret = process_active_file(sbuf, data);
                                break;
                        case DB_FTYPE_FINALISED:
                                ret = process_finalised_file(sbuf, data);
                                break;
                        case DB_FTYPE_PACKED:
                                ret = process_packed_file(sbuf, data);
                                break;
                        default:
                                break;

                        } /* switch() */
                } /* strncmp() */
                if (ret != ZS_OK) {
                        zslog(LOGDEBUG, "Failed processing DB %s\n", *path);
                        break;
                }
        } /* readdir() */

        closedir(dir);

        if (err)
                errno = err;

        err = ret;

        return err;
}
#endif  /* if defined(LINUX) || defined(DARWIN) || defined(BSD) */


static void zs_find_index_range_for_files(struct list_head *flist,
                                          uint32_t *startidx, uint32_t *endidx)
{
        struct list_head *pos;
        struct zsdb_file *first;

        *startidx = 0;
        *endidx = 0;

        /* The first entry in the list is the file with the latest
           (and the largest) index. */
        first = list_first(flist, struct zsdb_file, list);
        interpret_db_filename(first->fname.buf, strlen(first->fname.buf),
                              startidx, endidx);

        list_for_each_forward(pos, flist) {
                struct zsdb_file *f;
                uint32_t sidx = 0, eidx = 0;

                f = list_entry(pos, struct zsdb_file, list);

                interpret_db_filename(f->fname.buf, strlen(f->fname.buf),
                                      &sidx, &eidx);

                if (sidx < *startidx)
                        *startidx = sidx;

                if (eidx > *endidx)
                        *endidx = eidx;
        }
}

static int zsdb_reload_db(struct zsdb_priv *priv _unused_)
{
        return ZS_OK;
}

static int zs_iter_pq_cmp(const void *d1, const void *d2, void *cbdata _unused_)
{
        struct txn_data *e1, *e2;
        unsigned char *key1 = NULL, *key2 = NULL;
        uint64_t len1 = 0, len2 = 0;

        e1 = (struct txn_data *)d1;
        e2 = (struct txn_data *)d2;

        switch(e1->type) {
        case ZSDB_BE_ACTIVE:
        case ZSDB_BE_FINALISED:
        {
                struct btree_iter *iter = (struct btree_iter *)e1->data.iter;

                key1 = iter->record->key;
                len1 = iter->record->keylen;
        }
                break;
        case ZSDB_BE_PACKED:
                zs_packed_file_get_key_from_offset(e1->data.f, &key1, &len1);
                break;
        default:
                abort();        /* Should not happen */
                break;
        }

        switch(e2->type) {
        case ZSDB_BE_ACTIVE:
        case ZSDB_BE_FINALISED:
        {
                struct btree_iter *iter = (struct btree_iter *)e2->data.iter;

                key2 = iter->record->key;
                len2 = iter->record->keylen;
        }
                break;
        case ZSDB_BE_PACKED:
                zs_packed_file_get_key_from_offset(e2->data.f, &key1, &len1);
                break;
        default:
                break;
        }

        /* TODO: If key1 and key2 are the same, then check priority */
        return memcmp_raw(key1, len1, key2, len2);
}

struct txn_data *zs_txn_data_new(zsdb_be_t type, int prio, void *data)
{
        struct txn_data *d;

        d = xcalloc(1, sizeof(struct txn_data));

        d->type = type;
        d->priority = prio;
        if (type == ZSDB_BE_PACKED)
                d->data.f = data;
        else {
                struct btree *tree = data;
                btree_begin(tree, d->data.iter);
        }

        return d;
}

void zs_txn_data_free(struct txn_data **txnd)
{
        if (txnd && *txnd) {
                struct txn_data *data = *txnd;
                *txnd = NULL;

                xfree(data);
        }
}

int zsdb_transaction_begin(struct zsdb *db, struct txn **txn)
{
        struct txn *t = NULL;
        struct zsdb_priv *priv;
        struct list_head *pos;
        int prio;
        struct txn_data *ftxnd, *atxnd;

        assert_zsdb(db);

        priv = db->priv;
        if (!priv) return ZS_INTERNAL;

        if (!priv->open) {
                zslog(LOGWARNING, "DB `%s` not open!\n", priv->dbdir.buf);
                return ZS_NOT_OPEN;
        }

        t = xcalloc(1, sizeof(struct txn));

        t->db = db;
        t->pq.cmp = zs_iter_pq_cmp;

        /* Add the packed files to the PQ */
        list_for_each_forward(pos, &priv->dbfiles.pflist) {
                struct txn_data *txnd;
                struct zsdb_file *f;
                f = list_entry(pos, struct zsdb_file, list);
                txnd = zs_txn_data_new(ZSDB_BE_PACKED, f->priority, f);
                prio = f->priority;
                pqueue_put(&t->pq, txnd);
        }
        /* Add finalised record to the PQ */
        if (priv->dbfiles.ffcount) {
                prio++;
                ftxnd = zs_txn_data_new(ZSDB_BE_FINALISED, prio, priv->fmemtree);
                pqueue_put(&t->pq, ftxnd);
        }

        /* Add active record to the PQ */
        prio++;
        atxnd = zs_txn_data_new(ZSDB_BE_ACTIVE, prio, priv->memtree);
        pqueue_put(&t->pq, atxnd);

        *txn = t;
        return ZS_OK;
}

struct txn_data *zsdb_transaction_get(struct txn *txn)
{
        if (!txn)
                return NULL;

        assert_zsdb(txn->db);

        return pqueue_get(&txn->pq);
}

int zsdb_transaction_next(struct txn *txn, struct txn_data *data)
{

        if (!txn)
                return 0;

        assert_zsdb(txn->db);

        switch (data->type) {
        case ZSDB_BE_ACTIVE:
        case ZSDB_BE_FINALISED:
                if (!btree_next(data->data.iter)) {
                        zs_txn_data_free(&data);
                        return 0;
                }

                break;
        case ZSDB_BE_PACKED:
        {
                struct zsdb_file *f = data->data.f;
                f->indexpos++;
                if (f->indexpos > f->index->count) {
                        zs_txn_data_free(&data);
                        return 0;
                }
        }
                break;
        default:
                abort();        /* Should never reach here */
                break;
        }

        pqueue_put(&txn->pq, data);

        return 1;
}

void zsdb_transaction_end(struct txn **txn)
{
        struct txn *t;

        if (txn && *txn) {
                t = *txn;
                struct txn_data *txnd;
                *txn = NULL;

                t->db = NULL;

                while((txnd = pqueue_get(&t->pq))) {
                        zs_txn_data_free(&txnd);
                }

                pqueue_free(&t->pq);
                xfree(t);
        }
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
                cstring_release(&priv->dotzsdbfname);

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
        priv->dbfiles.pflist.prev = &priv->dbfiles.pflist;
        priv->dbfiles.pflist.next = &priv->dbfiles.pflist;

        priv->dbfiles.fflist.prev = &priv->dbfiles.fflist;
        priv->dbfiles.fflist.next = &priv->dbfiles.fflist;

        priv->dbfiles.afcount = 0;
        priv->dbfiles.ffcount = 0;
        priv->dbfiles.pfcount = 0;

        /* Compare functions for the pq for finalised and packed files */
        finalisedpq.cmp = dbfname_cmp;
        packedpq.cmp = dbfname_cmp;

        /* .zsdb filename */
        cstring_dup(&priv->dbdir, &priv->dotzsdbfname);
        cstring_addch(&priv->dotzsdbfname, '/');
        cstring_addstr(&priv->dotzsdbfname, DOTZSDB_FNAME);

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
        priv->fmemtree = btree_new(NULL, NULL);

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
                      priv->dbfiles.factive.fname.buf);
        } else {
                /* If it is an existing DB, scan the directory for
                 * db files.
                 */
                size_t mfsize = 0;
                struct list_head *pos;
                uint64_t priority;

                ret = for_each_db_file_in_dbdir(&priv->dbdir.buf,
                                                DB_ABS_PATH, priv);
                if (ret != ZS_OK)
                        goto done;

                /* Load records from active file to in-memory tree */
                zs_active_file_record_foreach(priv, load_records_cb,
                                              priv->memtree);

                /* Load data from finalised files */
                while (finalisedpq.count) {
                        struct zsdb_file *f = pqueue_get(&finalisedpq);
                        list_add_head(&f->list, &priv->dbfiles.fflist);
                        priv->dbfiles.ffcount++;
                }
                pqueue_free(&finalisedpq);

                if (priv->dbfiles.ffcount) {
                        priority = 0;
                        zslog(LOGDEBUG, "Loading data from finalised files\n");
                        list_for_each_reverse(pos, &priv->dbfiles.fflist) {
                                struct zsdb_file *f;
                                f = list_entry(pos, struct zsdb_file, list);
                                zslog(LOGDEBUG, "Loading %s\n", f->fname.buf);
                                zs_finalised_file_record_foreach(f,
                                                                 load_records_cb,
                                                                 priv->fmemtree);
                                f->priority = ++priority;
                        }
                }


                while (packedpq.count) {
                        struct zsdb_file *f = pqueue_get(&packedpq);
                        list_add_head(&f->list, &priv->dbfiles.pflist);
                        priv->dbfiles.pfcount++;
                }
                pqueue_free(&packedpq);


                /* Set priority of packed files */
                priority = 0;
                list_for_each_forward(pos, &priv->dbfiles.pflist) {
                        struct zsdb_file *f;
                        f = list_entry(pos, struct zsdb_file, list);
                        f->priority = ++priority;
                }

                /* Seek to the end of the file, that's where the
                   records need to appended to.
                */
                mappedfile_size(&priv->dbfiles.factive.mf, &mfsize);
                if (mfsize)
                        mappedfile_seek(&priv->dbfiles.factive.mf, mfsize, NULL);

                zslog(LOGDEBUG, "Found %d files in %s.\n",
                      priv->dbfiles.afcount +
                      priv->dbfiles.ffcount +
                      priv->dbfiles.pfcount,
                      priv->dbdir.buf);
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
        struct list_head *pos, *p;

        if (!db) {
                ret = ZS_IOERROR;
                goto done;
        }

        priv = db->priv;

        zslog(LOGDEBUG, "Closing DB `%s`.\n", priv->dbdir.buf);

        if (priv->dbfiles.factive.is_open)
                zsdb_write_lock_release(db);

        /* release locks */
        file_lock_release(&priv->plk);
        file_lock_release(&priv->wlk);

        zs_active_file_close(priv);

        list_for_each_forward_safe(pos, p, &priv->dbfiles.fflist) {
                struct zsdb_file *f;
                list_del(pos);
                f = list_entry(pos, struct zsdb_file, list);
                zs_finalised_file_close(&f);
                priv->dbfiles.ffcount--;
        }

        list_for_each_forward_safe(pos, p, &priv->dbfiles.pflist) {
                struct zsdb_file *f;
                list_del(pos);
                f = list_entry(pos, struct zsdb_file, list);
                zs_packed_file_close(&f);
                priv->dbfiles.pfcount--;
        }

        if (priv->memtree)
                btree_free(priv->memtree);

        if (priv->fmemtree)
                btree_free(priv->fmemtree);

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
        size_t mfsize = 0;
        struct record *rec;
        ino_t inonum;

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

        if (!priv->open || !priv->dbfiles.factive.is_open) {
                return ZS_NOT_OPEN;
        }

        if (!zsdb_write_lock_is_locked(db)) {
                zslog(LOGDEBUG, "Need a write lock to add records.\n");
                ret = ZS_ERROR;
                goto done;
        }

        inonum = zs_dotzsdb_get_ino(priv);
        if (inonum != priv->dotzsdb_ino) {
                /* If the inode number differ, the db has changed since the
                   time it has been opened. We need to reload the DB */
                zsdb_reload_db(priv); /* TODO: check return value */
                zslog(LOGWARNING, "DB updated!\n");
                ret = ZS_ERROR;
                goto done;
        }

        /* check file size and finalise if necessary */
        zslog(LOGWARNING, ">> Getting size for %s\n",
              priv->dbfiles.factive.fname.buf);
        if (mappedfile_size(&priv->dbfiles.factive.mf, &mfsize) != 0) {
                zslog(LOGWARNING, "Failed getting size for %s\n",
                        priv->dbfiles.factive.fname.buf);
                ret = ZS_ERROR;
                goto done;
        }
        zslog(LOGWARNING, ">> Size for %s = %zu\n",
              priv->dbfiles.factive.fname.buf, mfsize);

        if (mfsize >= TWOMB) {
                zslog(LOGDEBUG, "File %s is > 2MB, finalising.\n",
                        priv->dbfiles.factive.fname.buf);
                printf(">> File %s is > 2MB, finalising.\n",
                       priv->dbfiles.factive.fname.buf);
                ret = zs_active_file_finalise(priv);
                if (ret != ZS_OK) goto done;

                ret = zs_active_file_new(priv,
                                         priv->dotzsdb.curidx + 1);
                zslog(LOGDEBUG, "New active log file %s created.\n",
                        priv->dbfiles.factive.fname.buf);
                printf(">> New active log file %s created.\n",
                       priv->dbfiles.factive.fname.buf);
        }

        /* Start computing crc32, if we haven't already. The computation will
           end when the transaction is committed.
         */
        if (!priv->dbfiles.factive.mf->compute_crc)
                crc32_begin(&priv->dbfiles.factive.mf);

        /* Add the entry to the active file */
        ret = zs_active_file_write_keyval_record(priv, key, keylen, value,
                                                 vallen);
        if (ret != ZS_OK) {
                crc32_end(&priv->dbfiles.factive.mf);
                goto done;
        }
        priv->dbfiles.factive.dirty = 1;

        rec = record_new(key, keylen, value, vallen);
        btree_replace(priv->memtree, rec);

        /* TODO: REMOVE THE PRINTING! */
        /* btree_print_node_data(priv->memtree, NULL); */

        zslog(LOGDEBUG, "Inserted record into the DB. %s\n",
                priv->dbfiles.factive.fname.buf);

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

        if (!priv->open || !priv->dbfiles.factive.is_open) {
                return ZS_NOT_OPEN;
        }

        if (!zsdb_write_lock_is_locked(db)) {
                zslog(LOGDEBUG, "Need a write lock to remove records.\n");
                ret = ZS_ERROR;
                goto done;
        }

        /* Start computing the crc32. Will end when the transaction is
           committed */
        crc32_begin(&priv->dbfiles.factive.mf);

        ret = zs_active_file_write_delete_record(priv, key, keylen);
        if (ret != ZS_OK) {
                crc32_end(&priv->dbfiles.factive.mf);
                zslog(LOGDEBUG, "Failed removing key from DB `%s`\n",
                      priv->dbdir.buf);
                goto done;
        }
        priv->dbfiles.factive.dirty = 1;

        zslog(LOGDEBUG, "Removed key from DB `%s`\n", priv->dbdir.buf);

        /* Add the entry to the in-memory tree */
        rec = record_new(key, keylen, NULL, 0);
        btree_replace(priv->memtree, rec);

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

        if (!priv->dbfiles.factive.is_open)
                return ZS_NOT_OPEN;

        ret = zs_active_file_write_commit_record(priv);
        if (ret == ZS_OK)
                priv->dbfiles.factive.dirty = 0;

        return ret;
}

int zsdb_fetch(struct zsdb *db,
               unsigned char *key,
               size_t keylen,
               unsigned char **value,
               size_t *vallen)
{
        int ret = ZS_NOTFOUND;
        struct zsdb_priv *priv;
        btree_iter_t iter;

        assert_zsdb(db);
        assert(key);
        assert(keylen);

        priv = db->priv;

        if (!priv->open || !priv->dbfiles.factive.is_open) {
                zslog(LOGWARNING, "DB `%s` not open!\n", priv->dbdir.buf);
                return ZS_NOT_OPEN;
        }

        if (!key)
                return ZS_ERROR;

        /* Look for the key in the active in-memory btree */
        if (btree_find(priv->memtree, key, keylen, iter)) {
                /* We found the key in-memory */
                if (iter->record) {
                        unsigned char *v;
                        *vallen = iter->record->vallen;
                        v = xmalloc(*vallen);
                        memcpy(v, iter->record->val, *vallen);
                        *value = v;
                }

                ret = ZS_OK;
                goto done;
        }

        /* Look for the key in the finalised records */
        if (btree_find(priv->fmemtree, key, keylen, iter)) {
                /* We found the key in-memory */
                if (iter->record) {
                        unsigned char *v;
                        *vallen = iter->record->vallen;
                        v = xmalloc(*vallen);
                        memcpy(v, iter->record->val, *vallen);
                        *value = v;
                }

                ret = ZS_OK;
                goto done;
        }

        /* The key was not found in either the active file or the finalised
           files, look for it in the packed files */
        /* TODO: look for the key in the packed files */
        ret = ZS_NOTFOUND;

done:
        return ret;
}

static int print_btree_rec(struct record *record, void *data _unused_)
{
        size_t i;

        for (i = 0; i < record->keylen; i++) {
                printf("%c", record->key[i]);
        }
        if (record->keylen) printf("\n");

        for (i = 0; i < record->vallen; i++) {
                printf("%c", record->val[i]);
        }
        if (record->vallen) printf("\n");

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
                zslog(LOGWARNING, "DB `%s` not open!\n", priv->dbdir.buf);
                return ZS_NOT_OPEN;
        }

        if (level == DB_DUMP_ACTIVE || level == DB_DUMP_ALL) {
                if (level == DB_DUMP_ALL) {
                        struct txn *txn;
                        struct txn_data *data;

                        zsdb_transaction_begin(db, &txn);

                        data = zsdb_transaction_get(txn);
                        while (zsdb_transaction_next(txn, data)) {
                                switch (data->type) {
                                case ZSDB_BE_ACTIVE:
                                case ZSDB_BE_FINALISED:
                                        print_btree_rec(data->data.iter->record, NULL);
                                        break;
                                case ZSDB_BE_PACKED:
                                {
                                        struct zsdb_file *f = data->data.f;
                                        size_t offset = f->index->data[f->indexpos];
                                        zs_record_read_from_file(f, &offset,
                                                                 print_record_cb,
                                                                 NULL);
                                }
                                break;
                                default:
                                        break; /* Should never reach here */
                                }
                                data = zsdb_transaction_get(txn);
                        } /* while () */

                        zsdb_transaction_end(&txn);
                } else {
                        /* Dump active records only */
                        btree_walk_forward(priv->memtree, print_btree_rec, NULL);
#if 0
                        ret = zs_active_file_record_foreach(priv, print_rec,
                                                            NULL);
#endif
                }
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

/*
 * zsdb_repack(): Repack a DB
 * When the DB is being packed, records can be still be written to the
 * db, but there can only be one 'packing' process running at any given
 * moment.
 * Also, other processes that are writing to or reading from the db, should
 * check the inode number of the .zsdb file and if it has changed, since the
 * time the db was last opened, the process would have to close the db and
 * reopen again.
 */
int zsdb_repack(struct zsdb *db)
{
        int ret = ZS_OK;
        struct zsdb_priv *priv;
        ino_t inonum;
        uint32_t startidx, endidx;
        cstring fname = CSTRING_INIT;
        struct zsdb_file *f;
        struct list_head *pos, *p;

        assert_zsdb(db);

        priv = db->priv;
        if (!priv) return ZS_INTERNAL;

        if (!priv->open) {
                zslog(LOGWARNING, "DB `%s` not open!\n", priv->dbdir.buf);
                return ZS_NOT_OPEN;
        }

        if (!zsdb_pack_lock_is_locked(db)) {
                zslog(LOGDEBUG, "Need a pack lock to repack.\n");
                ret = ZS_ERROR;
                goto done;
        }

        inonum = zs_dotzsdb_get_ino(priv);
        if (inonum != priv->dotzsdb_ino) {
                /* If the inode numbers differ, the db has changed, since the
                   time it has been opened. We need to reload the DB */
                zsdb_reload_db(priv);
        }

        if (!zs_dotzsdb_update_begin(priv)) {
                zslog(LOGDEBUG, "Failed acquiring lock to repack!\n");
                ret = ZS_ERROR;
                goto done;
        }

        /* We prefer to pack finalised files first.
         */
        if (!list_empty(&priv->dbfiles.fflist)) {
                /* There are finalised files, which need to be packed, we do
                   that first */

                /* Find the index range */
                zs_find_index_range_for_files(&priv->dbfiles.fflist,
                                              &startidx, &endidx);

                zs_filename_generate_packed(priv, &fname, startidx, endidx);
                zslog(LOGDEBUG, "Packing into file %s...\n", fname.buf);
                /* Pack the records from the in-memory tree*/
                ret = zs_packed_file_new_from_memtree(fname.buf,
                                                      startidx, endidx,
                                                      priv, &f);
                if (ret != ZS_OK) {
                        crc32_end(&f->mf);
                        zslog(LOGDEBUG,
                              "Internal error when packing finalised files\n");
                        /* ERROR! */
                }

                zs_packed_file_close(&f);
                /* Close finalised files and unlink them */
                list_for_each_forward_safe(pos, p, &priv->dbfiles.fflist) {
                        struct zsdb_file *f;
                        list_del(pos);
                        f = list_entry(pos, struct zsdb_file, list);
                        xunlink(f->fname.buf);
                        zs_finalised_file_close(&f);
                        priv->dbfiles.ffcount--;
                }

                cstring_release(&fname);

                /* Done, for now. */
                goto done;
        }

        /* If there are no finalised files to be packed and we have more than
           1 packed files, we repack the packed files.
         */
        if (!list_empty(&priv->dbfiles.pflist) && (priv->dbfiles.pfcount > 1)) {
                zslog(LOGDEBUG, "Repacking packed files!\n");
        }

        zslog(LOGDEBUG, "Nothing to be packed for now!\n");
done:
        if (!zs_dotzsdb_update_end(priv)) {
                zslog(LOGDEBUG, "Failed release acquired lock for packing!\n");
                ret = ZS_ERROR;
        }

        return ret;
}

int zsdb_info(struct zsdb *db)
{
        int ret = ZS_OK;
        struct zsdb_priv *priv;
        struct list_head *pos;

        assert_zsdb(db);

        priv = db->priv;
        if (!priv) return ZS_INTERNAL;

        if (!priv->open) {
                zslog(LOGWARNING, "DB `%s` not open!\n", priv->dbdir.buf);
                return ZS_NOT_OPEN;
        }

        fprintf(stderr, "==============\n");
        fprintf(stderr, "== Zeroskip ==\n");
        fprintf(stderr, "==============\n");

        fprintf(stderr, "DBNAME  : %s\n", priv->dbdir.buf);
        fprintf(stderr, "UUID    : %s\n", priv->dotzsdb.uuidstr);
        fprintf(stderr, "Number of files: %d\n",
                priv->dbfiles.afcount +
                priv->dbfiles.ffcount +
                priv->dbfiles.pfcount);

        fprintf(stderr, ">> Active file:\n");
        fprintf(stderr, "\t * %s\n", basename(priv->dbfiles.factive.fname.buf));

        if (priv->dbfiles.ffcount) {
                fprintf(stderr, ">> Finalised file(s):\n");
                list_for_each_forward(pos, &priv->dbfiles.fflist) {
                        struct zsdb_file *f;
                        f = list_entry(pos, struct zsdb_file, list);
                        fprintf(stderr, "\t * [%3lu] %s\n",
                                f->priority, basename(f->fname.buf));
                }
        }

        if (priv->dbfiles.pfcount) {
                fprintf(stderr, ">> Packed file(s):\n");
                list_for_each_forward(pos, &priv->dbfiles.pflist) {
                        struct zsdb_file *f;
                        f = list_entry(pos, struct zsdb_file, list);
                        fprintf(stderr, "\t * [%3lu] %s\n",
                                f->priority, basename(f->fname.buf));
                }
        }

        return ret;
}

int zsdb_forone(struct zsdb *db _unused_, unsigned char *key _unused_,
                size_t keylen _unused_, foreach_p *p _unused_,
                foreach_cb *cb _unused_, struct txn **txn _unused_)
{
        return ZS_NOTIMPLEMENTED;
}

/* Lock file names */
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

        return (ret >= 0) ? ZS_OK : ZS_ERROR;
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
        int ret;

        assert(db);

        priv = db->priv;
        if (!priv) return ZS_INTERNAL;
        ret = file_lock_acquire(&priv->plk, priv->dbdir.buf,
                                PACK_LOCK_FNAME, timeout_ms);
        return (ret >= 0) ? ZS_OK : ZS_ERROR;
}

int zsdb_pack_lock_release(struct zsdb *db)
{
        struct zsdb_priv *priv;

        assert(db);

        priv = db->priv;
        if (!priv) return ZS_INTERNAL;

        return ((file_lock_release(&priv->plk) == 0) ? ZS_OK : ZS_ERROR);
}

int zsdb_pack_lock_is_locked(struct zsdb *db)
{
        struct zsdb_priv *priv;

        assert(db);

        priv = db->priv;
        if (!priv) return ZS_INTERNAL;
        return file_lock_is_locked(&priv->plk);
}
