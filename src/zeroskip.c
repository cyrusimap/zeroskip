/*
 * zeroskip
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */
#include "cstring.h"
#include "htable.h"
#include "pqueue.h"
#include <libzeroskip/btree.h>
#include <libzeroskip/log.h>
#include <libzeroskip/macros.h>
#include <libzeroskip/util.h>
#include <libzeroskip/zeroskip.h>
#include "zeroskip-priv.h"

#include <errno.h>

#if defined(LINUX) || defined(DARWIN) || defined(BSD)
#include <fts.h>
#else
#include <dirent.h>
#endif

#include <libgen.h>
#include <stdlib.h>
#include <inttypes.h>

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE
#endif

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

int zsdb_break(int err)
{
        return err;
}

/**
 * Private functions
 */
static int dbfname_cmp(const void *d1, const void *d2, void *cbdata _unused_)
{
        const struct zsdb_file *f1 = (const struct zsdb_file *)d1;
        const struct zsdb_file *f2 = (const struct zsdb_file *)d2;

        return natural_strcasecmp(f1->fname.buf, f2->fname.buf);
}

static int load_btree_record_cb(void *data,
                                const unsigned char *key, size_t keylen,
                                const unsigned char *value, size_t vallen)
{
        struct btree *memtree = (struct btree *)data;
        struct record *rec;

        rec = record_new(key, keylen, value, vallen, 0);
        btree_replace(memtree, rec);

        return 0;
}

static int load_deleted_btree_record_cb(void *data,
                                        const unsigned char *key, size_t keylen,
                                        const unsigned char *value, size_t vallen)
{
        struct btree *memtree = (struct btree *)data;
        struct record *rec;

        rec = record_new(key, keylen, value, vallen, 1);
        btree_replace(memtree, rec);

        return 0;
}

static int print_record_cb(void *data _unused_,
                           const unsigned char *key, size_t keylen,
                           const unsigned char *value, size_t vallen)
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
        char *p;
        char *idx;
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

#define MAX_BUF_PATH (2 * PATH_MAX)

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
                char sbuf[MAX_BUF_PATH];

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
                        snprintf(sbuf, MAX_BUF_PATH, "%s/%s/%s", buf,
                                 *path ? *path : buf, bname);
                else
                        snprintf(sbuf, MAX_BUF_PATH, "%s/%s", *path ? *path : buf, bname);

                if (strncmp(bname, ZS_FNAME_PREFIX, ZS_FNAME_PREFIX_LEN) == 0) {
                        switch(interpret_db_filename(bname, strlen(bname),
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
                char sbuf[MAX_BUF_PATH];
                struct stat sb;

                if (is_dotdir(de->d_name))
                        continue;

                bname = basename(de->d_name);

                if (full_path)
                        snprintf(sbuf, MAX_BUF_PATH, "%s/%s/%s", buf,
                                 *path ? *path : buf, bname);
                else
                        snprintf(sbuf, MAX_BUF_PATH, "%s/%s",
                                 *path ? *path : buf, bname);

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

static int zsdb_reload(struct zsdb_priv *priv)
{
        int ret = ZS_OK;
        struct list_head *pos, *p;
        size_t mfsize;
        uint64_t priority = 0;

        if (!priv->open) {
                zslog(LOGWARNING, "DB not open!\n");
                return ZS_ERROR;
        }

        /** Close all files **/
        /* Close active */
        zs_active_file_close(priv);

        /* Close finalised file */
        list_for_each_forward_safe(pos, p, &priv->dbfiles.fflist) {
                struct zsdb_file *f;
                list_del(pos);
                f = list_entry(pos, struct zsdb_file, list);
                zs_finalised_file_close(&f);
                priv->dbfiles.ffcount--;
        }

        /* Close packed files */
        list_for_each_forward_safe(pos, p, &priv->dbfiles.pflist) {
                struct zsdb_file *f;
                list_del(pos);
                f = list_entry(pos, struct zsdb_file, list);
                zs_packed_file_close(&f);
                priv->dbfiles.pfcount--;
        }

        if (priv->memtree) {
                btree_free(priv->memtree);
                priv->memtree = NULL;
        }

        if (priv->fmemtree) {
                btree_free(priv->fmemtree);
                priv->fmemtree = NULL;
        }

        /* Allocate In-memory tree */
        priv->memtree = btree_new(NULL, priv->btcompare);
        priv->fmemtree = btree_new(NULL, priv->btcompare);

        /** Reopen/Reload all files */
        ret = for_each_db_file_in_dbdir(&priv->dbdir.buf, DB_ABS_PATH,
                                        priv);
        if (ret != ZS_OK)
                goto done;

        /* Load records from active file to in-memory tree */
        ret = zs_active_file_record_foreach(priv, load_btree_record_cb,
                                            load_deleted_btree_record_cb,
                                            priv->memtree);
        if (ret != ZS_OK)
                goto done;

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
                                                         load_btree_record_cb,
                                                         load_deleted_btree_record_cb,
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
done:
        return ret;
}

/**
 * Public functions
 */
int zsdb_init(struct zsdb **pdb, zsdb_cmp_fn dbcmpfn,
              btree_search_cb_t btcmpfn)
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

        if (dbcmpfn)
                priv->dbcompare = dbcmpfn;

        if (btcmpfn)
                priv->btcompare = btcmpfn;

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
        struct stat sb;
        mode_t mode = 0777;

        memset(&sb, 0, sizeof(struct stat));

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
        struct stat sb;
        int newdb = 0;

        memset(&sb, 0, sizeof(struct stat));

        assert(db);
        assert(db->priv);
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

        if (mode & MODE_CUSTOMSEARCH) {
                /* CUSTOMSEARCH mode needs to have custom compare functions */
                assert(priv->dbcompare);
                assert(priv->btcompare);
        }

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
                if (mode & MODE_CREATE) {
                        ret = zsdb_create(db);
                        newdb = 1;
                } else {
                        ret = ZS_NOTFOUND;
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
        priv->memtree = btree_new(NULL, priv->btcompare);
        priv->fmemtree = btree_new(NULL, priv->btcompare);

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
                ret = zs_active_file_record_foreach(priv, load_btree_record_cb,
                                                    load_deleted_btree_record_cb,
                                                    priv->memtree);
                if (ret != ZS_OK)
                        goto done;

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
                                                                 load_btree_record_cb,
                                                                 load_deleted_btree_record_cb,
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
             const unsigned char *key,
             size_t keylen,
             const unsigned char *value,
             size_t vallen,
             struct zsdb_txn **txn _unused_)
{
        int ret = ZS_OK;
        struct zsdb_priv *priv;
        size_t mfsize = 0;
        struct record *rec;
        ino_t inonum;
        const unsigned char *empty = (const unsigned char *)"";

        assert(db);
        assert(db->priv);
        assert(key);
        assert(keylen);

        if (vallen)
                assert(value);

        if (!db)
                return ZS_NOT_OPEN;

        if (!key)
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
                ret = zsdb_reload(priv);
                if (ret != ZS_OK) {
                        zslog(LOGWARNING, "Failed reoloading DB!\n");
                        goto done;
                }
                zslog(LOGDEBUG, "Reloaded DB!\n");
        }

        /* check file size and finalise if necessary */
        if (mappedfile_size(&priv->dbfiles.factive.mf, &mfsize) != 0) {
                zslog(LOGWARNING, "Failed getting size for %s\n",
                        priv->dbfiles.factive.fname.buf);
                ret = ZS_ERROR;
                goto done;
        }

        if (mfsize >= TWOMB) {
                zslog(LOGDEBUG, "File %s is > 2MB, finalising.\n",
                        priv->dbfiles.factive.fname.buf);
                ret = zs_active_file_finalise(priv);
                if (ret != ZS_OK) goto done;

                ret = zs_active_file_new(priv,
                                         priv->dotzsdb.curidx + 1);
                zslog(LOGDEBUG, "New active log file %s created.\n",
                        priv->dbfiles.factive.fname.buf);
        }

        /* Start computing crc32, if we haven't already. The computation will
           end when the transaction is committed.
         */
        if (!priv->dbfiles.factive.mf->compute_crc)
                crc32_begin(&priv->dbfiles.factive.mf);

        /* Add the entry to the active file */
        ret = zs_active_file_write_keyval_record(priv, key, keylen,
                                                 (value ? value : empty),
                                                 vallen);
        if (ret != ZS_OK) {
                crc32_end(&priv->dbfiles.factive.mf);
                goto done;
        }
        priv->dbfiles.factive.dirty = 1;

        rec = record_new(key, keylen, value, vallen, 0);
        btree_replace(priv->memtree, rec);

        zslog(LOGDEBUG, "Inserted record into the DB. %s\n",
                priv->dbfiles.factive.fname.buf);

done:
        return ret;
}

int zsdb_remove(struct zsdb *db,
                const unsigned char *key, size_t keylen,
                struct zsdb_txn **txn _unused_)
{
        int ret = ZS_OK;
        struct zsdb_priv *priv;
        struct record *rec;

        assert(db);
        assert(db->priv);
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
        rec = record_new(key, keylen, NULL, 0, 1);
        btree_replace(priv->memtree, rec);

done:
        return ret;
}

int zsdb_commit(struct zsdb *db, struct zsdb_txn *txn)
{
        int ret = ZS_OK;
        struct zsdb_priv *priv;

        assert(db);
        assert(db->priv);

        priv = db->priv;

        if (!priv->dbfiles.factive.is_open)
                return ZS_NOT_OPEN;

        if (!priv->dbfiles.factive.mf->crc32_data_len &&
            !priv->dbfiles.factive.dirty) {
                goto done;
        }

        ret = zs_active_file_write_commit_record(priv);
        if (ret == ZS_OK)
                priv->dbfiles.factive.dirty = 0;

        /* Update the index and offset in the .zsdb file */
        zs_dotzsdb_update_index_and_offset(priv, priv->dotzsdb.curidx,
               priv->dbfiles.factive.mf->offset);

done:
        if (txn) {
                zs_transaction_end(&txn);
                txn = NULL;
        }

        return ret;
}

int zsdb_fetch(struct zsdb *db,
               const unsigned char *key,
               size_t keylen,
               const unsigned char **value,
               size_t *vallen,
               struct zsdb_txn **txn _unused_)
{
        int ret = ZS_NOTFOUND;
        struct zsdb_priv *priv;
        btree_iter_t iter;
        struct list_head *pos;

        assert(db);
        assert(db->priv);
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
        zslog(LOGDEBUG, "Looking in active records\n");
        if (btree_find(priv->memtree, key, keylen, iter)) {
                /* We found the key in active records */
                if (iter->record && !iter->record->deleted) {
                        *vallen = iter->record->vallen;
                        *value = iter->record->val;
                        ret = ZS_OK;
                        goto done;
                }
        }

        /* Look for the key in the finalised records */
        zslog(LOGDEBUG, "Looking in finalised file(s)\n");
        if (btree_find(priv->fmemtree, key, keylen, iter)) {
                /* We found the key in finalised records */
                if (iter->record) {
                        *vallen = iter->record->vallen;
                        *value = iter->record->val;
                }

                ret = ZS_OK;
                goto done;
        }

        /* The key was not found in either the active file or the finalised
           files, look for it in the packed files */
        zslog(LOGDEBUG, "Looking in the Packed file(s)\n");
        list_for_each_forward(pos, &priv->dbfiles.pflist) {
                struct zsdb_file *f;
                uint64_t location = 0;
                int cmp_ret;
                struct zs_key temp_key;

                f = list_entry(pos, struct zsdb_file, list);

                zslog(LOGDEBUG, "Looking in packed file %s\n",
                      f->fname.buf);
                zslog(LOGDEBUG, "\tTotal records: %d\n",
                      f->index->count);

                /* If the given key is smaller than the smallest key in the
                 * packedfile or bigger than the the biggest key, we continue
                 * to the next file in the list instead of binary searching
                 * in the current file.
                 */
                zslog(LOGDEBUG, "\tfirst record at offset: %d\n",
                      f->index->data[0]);
                zs_record_read_key_from_file_offset(f, f->index->data[0],
                                                    &temp_key);
                if (priv->dbcompare) {
                        cmp_ret = priv->dbcompare(key, keylen, temp_key.data,
                                                  (temp_key.base.type == REC_TYPE_KEY ||
                                                   temp_key.base.type == REC_TYPE_DELETED) ?
                                                  temp_key.base.slen : temp_key.base.slen);
                } else {
                        cmp_ret = memcmp_raw(key, keylen, temp_key.data,
                                             (temp_key.base.type == REC_TYPE_KEY ||
                                              temp_key.base.type == REC_TYPE_DELETED) ?
                                             temp_key.base.slen : temp_key.base.slen);
                }
                if (cmp_ret < 0)
                        continue;

                zslog(LOGDEBUG, "\tlast record at offset: %d\n",
                      f->index->data[f->index->count - 1]);
                zs_record_read_key_from_file_offset(f,
                                                    f->index->data[f->index->count - 1],
                                                    &temp_key);
                if (priv->dbcompare) {
                        cmp_ret = priv->dbcompare(key, keylen, temp_key.data,
                                                  (temp_key.base.type == REC_TYPE_KEY ||
                                                   temp_key.base.type == REC_TYPE_DELETED) ?
                                                  temp_key.base.slen : temp_key.base.slen);
                } else {
                        cmp_ret = memcmp_raw(key, keylen, temp_key.data,
                                             (temp_key.base.type == REC_TYPE_KEY ||
                                              temp_key.base.type == REC_TYPE_DELETED) ?
                                             temp_key.base.slen : temp_key.base.slen);
                }
                if (cmp_ret > 0)
                        continue;

                if (zs_packed_file_bsearch_index(key, keylen, f, &location,
                                                 value, vallen, priv->dbcompare)) {
                        zslog(LOGDEBUG, "Record found at location %ld\n",
                              location);
                        ret = ZS_OK;
                        goto done;
                }
        }

        ret = ZS_NOTFOUND;

done:
        return ret;
}

int zsdb_fetchnext(struct zsdb *db,
                   const unsigned char *key, size_t keylen,
                   const unsigned char **found, size_t *foundlen,
                   const unsigned char **value, size_t *vallen,
                   struct zsdb_txn **txn)
{
        int ret = ZS_OK;
        struct zsdb_priv *priv;
        struct zsdb_iter *tempiter = NULL;
        int keyfound = 0;
        struct zsdb_iter_data *data;

        assert(db);
        assert(db->priv);

        if (keylen)
                assert(key);

        if (db)
                priv = db->priv;

        if (!priv->open) {
                zslog(LOGWARNING, "DB `%s` not open!\n", priv->dbdir.buf);
                return ZS_NOT_OPEN;
        }

        /* Create a new iterator */
        zs_iterator_new(db, &tempiter);

        ret = zs_iterator_begin_at_key(&tempiter, key, keylen, &keyfound);
        if (ret != ZS_OK) {
                goto fail;
        }

        data = zs_iterator_get(tempiter);

        if (keyfound) {
                /* If we found the key, we go to the next key in the iterator */
                if (!zs_iterator_next(tempiter, data)) {
                        ret = ZS_NOTFOUND;
                        goto done;
                }

                data = zs_iterator_get(tempiter);
        }

        /* Return data */
        switch(data->type) {
        case ZSDB_BE_ACTIVE:
        case ZSDB_BE_FINALISED:
                *found = data->data.iter->record->key;
                *foundlen = data->data.iter->record->keylen;
                *value = data->data.iter->record->val;
                *vallen = data->data.iter->record->vallen;
                break;
        case ZSDB_BE_PACKED:
        {
                struct zsdb_file *f = data->data.f;
                size_t offset = f->index->data[f->indexpos];

                zs_record_read_key_val_from_offset(f, &offset,
                                                   found, foundlen,
                                                   value, vallen);
        }
                break;
        default:
                abort();
        }

        if (txn && *txn && (*txn)->alloced)
                (*txn)->iter = tempiter;
        else
                zs_iterator_end(&tempiter);

        goto done;
fail:
        ret = ZS_ERROR;
        zs_iterator_end(&tempiter);
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

int zsdb_dump(struct zsdb *db, DBDumpLevel level)
{
        int ret = ZS_OK;
        struct zsdb_priv *priv;
        int count = 0;

        assert(db);
        assert(db->priv);

        if (db)
                priv = db->priv;

        if (!priv->open) {
                zslog(LOGWARNING, "DB `%s` not open!\n", priv->dbdir.buf);
                return ZS_NOT_OPEN;
        }

        if (level == DB_DUMP_ACTIVE || level == DB_DUMP_ALL) {
                if (level == DB_DUMP_ALL) {
                        struct zsdb_iter *iter = NULL;
                        struct zsdb_iter_data *idata;

                        zs_iterator_new(db, &iter);

                        if (zs_iterator_begin(&iter) != ZS_OK)
                                return ZS_INTERNAL;

                        do {
                                idata = zs_iterator_get(iter);
                                if (!idata)
                                        break;

                                if (idata->deleted) /* deleted key */
                                        continue;

                                switch (idata->type) {
                                case ZSDB_BE_ACTIVE:
                                case ZSDB_BE_FINALISED:
                                        print_btree_rec(idata->data.iter->record, NULL);
                                        break;
                                case ZSDB_BE_PACKED:
                                {
                                        struct zsdb_file *f = idata->data.f;
                                        size_t offset = f->index->data[f->indexpos];
                                        zs_record_read_from_file(f, &offset,
                                                                 print_record_cb,
                                                                 NULL,
                                                                 NULL);
                                }
                                break;
                                default:
                                        break; /* Should never reach here */
                                }
                                count++;
                        } while (zs_iterator_next(iter, idata));

                        zs_iterator_end(&iter);
                } else {
                        /* Dump active records only */
                        btree_walk_forward(priv->memtree, print_btree_rec, NULL);
                        count = priv->memtree->count;
                }
        } else {
                zslog(LOGDEBUG, "Invalid DB dump option\n");
                return ZS_ERROR;
        }

        zslog(LOGDEBUG, ">> Total records: %d\n", count);
        return ret;
}

int zsdb_abort(struct zsdb *db, struct zsdb_txn **txn _unused_)
{
        int ret = ZS_NOTIMPLEMENTED;
        struct zsdb_priv *priv;

        assert(db);
        assert(db->priv);

        if (db)
                priv = db->priv;

        if (!priv->open) {
                zslog(LOGWARNING, "DB `%s` not open!\n", priv->dbdir.buf);
                return ZS_NOT_OPEN;
        }

        zslog(LOGDEBUG, "Aborting transaction!\n");

        /* Truncate the active file until the last known valid offset as
           stored in priv->dotzsdb.offset
        */
        if (mappedfile_truncate(&priv->dbfiles.factive.mf,
                                priv->dotzsdb.offset) != 0) {
                zslog(LOGWARNING, "Failed truncating file %s to offset %lu",
                      priv->dbfiles.factive.fname.buf, priv->dotzsdb.offset);
        }

        priv->dbfiles.factive.mf->offset = priv->dotzsdb.offset;

        /* Mark the active file as *NOT* dirty, so that we don't
         * write the commit record */
        priv->dbfiles.factive.dirty = 0;

        /* End the current transaction */
        if (txn && *txn && (*txn)->alloced)
                zs_transaction_end(txn);

        /* Need to reload the DB, since the in-memory tree has changed. */
        ret = zsdb_write_lock_acquire(db, 0);
        assert(ret == ZS_OK);

        ret = zsdb_reload(priv);
        if (ret != ZS_OK) {
                zslog(LOGWARNING, "Failed reloading DB during abort!\n");
                goto done;
        }

        ret = zsdb_write_lock_release(db);
        assert(ret == ZS_OK);

        ret = ZS_OK;
done:
        return ret;
}

int zsdb_consistent(struct zsdb *db, struct zsdb_txn **txn _unused_)
{
        int ret = ZS_NOTIMPLEMENTED;

        assert(db);
        assert(db->priv);

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
        struct list_head *pos, *p;

        assert(db);
        assert(db->priv);

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
                ret = zsdb_reload(priv);
                if (ret != ZS_OK) {
                        zslog(LOGWARNING, "Failed reoloading DB!\n");
                        goto done;
                }
                zslog(LOGDEBUG, "Reloaded DB!\n");
        }

        if (!zs_dotzsdb_update_begin(priv)) {
                zslog(LOGDEBUG, "Failed acquiring lock to repack!\n");
                ret = ZS_ERROR;
                goto done;
        }

        /* We prefer to pack finalised files first.
         */
        if (!list_empty(&priv->dbfiles.fflist)) {
                struct zsdb_file *f;
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

                priv->dbfiles.pfcount++;

                /* Close finalised files and unlink them */
                list_for_each_forward_safe(pos, p, &priv->dbfiles.fflist) {
                        struct zsdb_file *tempf;
                        list_del(pos);
                        tempf = list_entry(pos, struct zsdb_file, list);
                        xunlink(tempf->fname.buf);
                        zs_finalised_file_close(&tempf);
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
                struct list_head filelist;
                struct zsdb_file *newpfile = NULL;
                struct zsdb_iter *iter = NULL;
                int i = 0;

                filelist.prev = &filelist;

                zslog(LOGDEBUG, "Repacking packed files:\n");
                list_for_each_forward_safe(pos, p,  &priv->dbfiles.pflist) {
                        struct zsdb_file *tempf;
                        tempf = list_entry(pos, struct zsdb_file, list);
                        if (i == 2) break; /* Pack the 2 oldest files */

                        list_del(pos);
                        list_add_head(&tempf->list, &filelist);
                        printf("\t > %s\n", tempf->fname.buf);
                        i++;
                }

                /* Find the index range */
                zs_find_index_range_for_files(&filelist,
                                              &startidx, &endidx);

                zs_filename_generate_packed(priv, &fname, startidx, endidx);
                zslog(LOGDEBUG, "Packing into file %s...\n", fname.buf);

                ret = zs_iterator_new(db, &iter);
                ret = zs_packed_file_new_from_packed_files(fname.buf,
                                                           startidx, endidx,
                                                           priv, &filelist,
                                                           &iter, &newpfile);
                zs_iterator_end(&iter);

                zs_packed_file_close(&newpfile);

                priv->dbfiles.pfcount++;

                list_for_each_forward_safe(pos, p, &filelist) {
                        struct zsdb_file *tempf;
                        list_del(pos);
                        tempf = list_entry(pos, struct zsdb_file, list);
                        xunlink(tempf->fname.buf);
                        zs_packed_file_close(&tempf);
                        priv->dbfiles.pfcount--;
                }

                /* Done, for now. */
                goto done;
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

        assert(db);
        assert(db->priv);

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
                        fprintf(stderr, "\t * [%3" PRIu64 "] %s\n",
                                f->priority, basename(f->fname.buf));
                }
        }

        if (priv->dbfiles.pfcount) {
                fprintf(stderr, ">> Packed file(s):\n");
                list_for_each_forward(pos, &priv->dbfiles.pflist) {
                        struct zsdb_file *f;
                        f = list_entry(pos, struct zsdb_file, list);
                        fprintf(stderr, "\t * [%3" PRIu64 "] %s\n",
                                f->priority, basename(f->fname.buf));
                }
        }

        return ret;
}

int zsdb_finalise(struct zsdb *db)
{
        int ret = ZS_OK;
        struct zsdb_priv *priv;
        ino_t inonum;

        assert(db);
        assert(db->priv);

        if (!db)
                return ZS_NOT_OPEN;

        priv = db->priv;

        if (!priv->open || !priv->dbfiles.factive.is_open) {
                return ZS_NOT_OPEN;
        }

        if (!zsdb_write_lock_is_locked(db)) {
                zslog(LOGDEBUG, "Need a write lock to finalise records.\n");
                ret = ZS_ERROR;
                goto done;
        }

        inonum = zs_dotzsdb_get_ino(priv);
        if (inonum != priv->dotzsdb_ino) {
                /* If the inode number differ, the db has changed since the
                   time it has been opened. We need to reload the DB */
                ret = zsdb_reload(priv);
                if (ret != ZS_OK) {
                        zslog(LOGWARNING, "Failed reoloading DB!\n");
                        goto done;
                }
                zslog(LOGDEBUG, "Reloaded DB!\n");
        }

        zslog(LOGDEBUG, "Finalising %s.\n",
              priv->dbfiles.factive.fname.buf);
        ret = zs_active_file_finalise(priv);
        if (ret != ZS_OK) goto done;

        ret = zs_active_file_new(priv,
                                 priv->dotzsdb.curidx + 1);
        zslog(LOGDEBUG, "New active log file %s created.\n",
              priv->dbfiles.factive.fname.buf);

        /* Start computing crc32, if we haven't already. The computation will
           end when the transaction is committed.
         */
        if (!priv->dbfiles.factive.mf->compute_crc)
                crc32_begin(&priv->dbfiles.factive.mf);

done:
        return ret;
}

int zsdb_foreach(struct zsdb *db, const unsigned char *prefix, size_t prefixlen,
                 zsdb_foreach_p *p, zsdb_foreach_cb *cb, void *cbdata,
                 struct zsdb_txn **txn)
{
        int ret = ZS_OK;
        struct zsdb_priv *priv;
        struct zsdb_iter_data *data;
        struct zsdb_iter *tempiter = NULL;
        int newtxn = 0;
        int found = 0;

        assert(db);
        assert(db->priv);

        if (db)
                priv = db->priv;

        if (!priv->open) {
                zslog(LOGWARNING, "DB `%s` not open!\n", priv->dbdir.buf);
                return ZS_NOT_OPEN;
        }

        if (prefixlen) {
                assert(prefix);
        }

        if (txn && *txn && (*txn)->iter) {  /* Existing transaction */
                tempiter = (*txn)->iter;
        } else {                /* New transaction */
                zs_iterator_new(db, &tempiter);

                if (prefix)
                        ret = zs_iterator_begin_at_key(&tempiter,
                                                       prefix,
                                                       prefixlen, &found);
                else
                        zs_iterator_begin(&tempiter);

                newtxn = 1;
        }

        do {
                const unsigned char *key = NULL, *val = NULL;
                size_t keylen = 0, vallen = 0;

                data = zs_iterator_get(tempiter);
                if (!data)
                        break;

                if (data->deleted) /* deleted key */
                        continue;

                switch (data->type) {
                case ZSDB_BE_ACTIVE:
                case ZSDB_BE_FINALISED:
                        key = data->data.iter->record->key;
                        keylen = data->data.iter->record->keylen;
                        val = data->data.iter->record->val;
                        vallen = data->data.iter->record->vallen;
                        break;
                case ZSDB_BE_PACKED:
                {
                        struct zsdb_file *f = data->data.f;
                        size_t offset = f->index->data[f->indexpos];

                        zs_record_read_key_val_from_offset(f, &offset,
                                                           &key, &keylen,
                                                           &val, &vallen);
                }
                break;
                default:
                        abort(); /* Should never reach here */
                }

                /* If there is a prefix, then ensure we match it */
                if (prefixlen) {
                        int r;

                        if (priv->dbcompare)
                                r = priv->dbcompare(key, prefixlen, prefix, prefixlen);
                        else
                                r = memcmp_raw(key, prefixlen, prefix, prefixlen);

                        if (r)
                                break;
                }

                if (!p || p(cbdata, key, keylen, val, vallen)) {
                        if (cb(cbdata, key, keylen, val, vallen))
                                break;
                }

        } while (zs_iterator_next(tempiter, data));

        if (newtxn)
                zs_iterator_end(&tempiter);

        return ret;
}

int zsdb_forone(struct zsdb *db, const unsigned char *key, size_t keylen,
                zsdb_foreach_p *p, zsdb_foreach_cb *cb, void *cbdata,
                struct zsdb_txn **txn)
{
        int ret = ZS_OK;
        struct zsdb_priv *priv;
        struct zsdb_iter *tempiter = NULL;
        int found = 0;
        unsigned char *val = NULL;
        size_t vallen = 0;
        struct zs_key krec;
        struct zs_val vrec;


        assert(db);
        assert(db->priv);
        assert(key);
        assert(keylen);

        if (db)
                priv = db->priv;

        if (!priv->open) {
                zslog(LOGWARNING, "DB `%s` not open!\n", priv->dbdir.buf);
                return ZS_NOT_OPEN;
        }

        /* Create a new iterator */
        zs_iterator_new(db, &tempiter);
        tempiter->forone_iter = 1;

        ret = zs_iterator_begin_at_key(&tempiter, key, keylen, &found);
        if (ret != ZS_OK) {
                goto fail;
        }

        if (!found) {
                goto fail;
        } else {
                struct zsdb_iter_data *data;
                data = zs_iterator_get(tempiter);

                switch(data->type) {
                case ZSDB_BE_ACTIVE:
                case ZSDB_BE_FINALISED:
                        val = data->data.iter->record->val;
                        vallen = data->data.iter->record->vallen;
                        break;
                case ZSDB_BE_PACKED:
                {
                        struct zsdb_file *f = data->data.f;
                        size_t offset = f->index->data[f->indexpos];

                        zs_read_key_val_record_from_file_offset(f, &offset,
                                                                &krec, &vrec);
                        val = vrec.data;
                        vallen = vrec.base.type == REC_TYPE_VALUE ?
                                vrec.base.slen : vrec.base.llen;
                }
                break;
                default:
                        abort();
                }

                if (!p || p(cbdata, key, keylen, val, vallen))
                        ret = cb(cbdata, key, keylen, val, vallen);

                /* We've already got the key we wanted,
                 * iterate to the next item */
                zs_iterator_next(tempiter, data);

                if (txn && *txn && (*txn)->alloced)
                        (*txn)->iter = tempiter;

                goto done;
        }

fail:
        ret = 0;
        zs_iterator_end(&tempiter);
done:
        return ret;
}

int zsdb_transaction_begin(struct zsdb *db, struct zsdb_txn **txn)
{
        return zs_transaction_begin(db, txn);
}

void zsdb_transaction_end(struct zsdb_txn **txn)
{
        zs_transaction_end(txn);
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
