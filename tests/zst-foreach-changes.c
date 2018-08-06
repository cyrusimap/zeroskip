/*
 * zeroskip
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 * Copyright (c) 2017 Partha Susarla <mail@spartha.org>
 */

#include <libzeroskip/zeroskip.h>
#include <libzeroskip/macros.h>
#include <libzeroskip/log.h>
#include <libzeroskip/util.h>

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define DBNAME "ZSTESTDB"

struct ffrock {
        struct zsdb *db;
        struct zsdb_txn **tid;
        int state;
};


#define K0 "affect"
#define K0U "bother"
#define K1 "carib"
#define K2 "cubist"
#define K3 "eulogy"
#define K4 "kidding"
#define K4A "llama"
#define K5 "monkey"
#define K6 "notice"
#define K7 "octopus"
#define K7D "opossum"
#define K7A "possum"
#define K7B "quine"
#define K8 "rooster"

static struct {
        const unsigned char *k;
        size_t klen;
        const unsigned char *v;
        size_t vlen;
} kvrecs[] = {
        { (const unsigned char *)K1, 5, (const unsigned char *)"delays maj bullish packard ronald", 33 },
        { (const unsigned char *)K2, 6, (const unsigned char *)"bobby tswana cu albumin created", 31 },
        { (const unsigned char *)K3, 6, (const unsigned char *)"aleut stoic muscovy adonis moe docent", 37 },
        { (const unsigned char *)K4, 7, (const unsigned char *)"curry deterrent drove raising hiring", 36 },
        { (const unsigned char *)K5, 6, (const unsigned char *)"joining keeper angle burden buffer", 34 },
        { (const unsigned char *)K6, 6, (const unsigned char *)"annoying push security plenty ending", 36 },
};


static int fe_cb(void *data,
                 const unsigned char *key, size_t keylen,
                 const unsigned char *val _unused_,
                 size_t vallen _unused_)
{
        struct ffrock *fr = (struct ffrock *)data;
        int r;

        switch (fr->state) {
        case 0:
                r = memcmp(key, K1, keylen);
                assert(r == 0);
                fr->state = 1;
                break;
        case 1:
                r = memcmp(key, K2, keylen);
                assert(r == 0);
                zsdb_write_lock_acquire(fr->db, 0);
                r = zsdb_add(fr->db, (const unsigned char *)K0, strlen(K0),
                             (const unsigned char *)"", 0, fr->tid);
                assert(r == ZS_OK);
                zsdb_write_lock_release(fr->db);
                fr->state = 2;
                break;
        case 2:
                r = memcmp(key, K3, keylen);
                assert(r == 0);
                r = zsdb_fetch(fr->db, (const unsigned char *)K0U, strlen(K0U),
                               NULL, NULL, fr->tid);
                assert(r == ZS_NOTFOUND);
                fr->state = 3;
                break;
        case 3:
                r = memcmp(key, K4, keylen);
                assert(r == 0);
                zsdb_write_lock_acquire(fr->db, 0);
                r = zsdb_add(fr->db, (const unsigned char *)K4A, strlen(K4A),
                             (const unsigned char *)"", 0, fr->tid);
                assert(r == ZS_OK);
                zsdb_write_lock_release(fr->db);
                fr->state = 4;
                break;
        case 4:
                r = memcmp(key, K4A, keylen);
                assert(r == 0);
                zsdb_write_lock_acquire(fr->db, 0);
                r = zsdb_add(fr->db, (const unsigned char *)K4A, strlen(K4A),
                             (const unsigned char *)"another", 7, fr->tid);
                assert(r == ZS_OK);
                zsdb_write_lock_release(fr->db);
                fr->state = 5;
                break;
        case 5:
                r = memcmp(key, K5, keylen);
                assert(r == 0);
                zsdb_write_lock_acquire(fr->db, 0);
                r = zsdb_remove(fr->db, (const unsigned char *)K5,
                                strlen(K5), fr->tid);
                assert(r == ZS_OK);
                zsdb_write_lock_release(fr->db);
                fr->state = 6;
                break;
        case 6:
                r = memcmp(key, K6, keylen);
                assert(r == 0);
                fr->state = 7;
                break;
        case 7:
                r = memcmp(key, K7, keylen);
                assert(r == 0);

                zsdb_write_lock_acquire(fr->db, 0);
                r = zsdb_add(fr->db, (const unsigned char *)K7, strlen(K7),
                             (const unsigned char *)"newval", 6, fr->tid);
                assert(r == ZS_OK);

                r = zsdb_add(fr->db, (const unsigned char *)K7D, strlen(K7D),
                             (const unsigned char *)"val", 3, fr->tid);
                assert(r == ZS_OK);

                r = zsdb_add(fr->db, (const unsigned char *)K7B, strlen(K7B),
                             (const unsigned char *)"bval", 4, fr->tid);
                assert(r == ZS_OK);

                r = zsdb_add(fr->db, (const unsigned char *)K7A, strlen(K7A),
                             (const unsigned char *)"aval", 4, fr->tid);
                assert(r == ZS_OK);


                r = zsdb_remove(fr->db, (const unsigned char *)K7D,
                                strlen(K7D), fr->tid);
                assert(r == ZS_OK);

                zsdb_write_lock_release(fr->db);
                fr->state = 8;
                break;
        case 8:
                r = memcmp(key, K7A, keylen);
                assert(r == 0);

                fr->state = 9;
                break;
        case 9:
                r = memcmp(key, K7B, keylen);
                assert(r == 0);

                fr->state = 10;
                break;
        case 10:
                r = memcmp(key, K7B, keylen);
                assert(r == 0);

                fr->state = 11;
                break;
        default:
                assert(0);
                break;
        }

        return 0;
}

static int add_records(struct zsdb *db, struct zsdb_txn *txn)
{
        size_t i;
        int ret = ZS_OK;

        /* Begin transaction */
        ret = zsdb_transaction_begin(db, &txn);
        if (ret != ZS_OK) {
                zslog(LOGWARNING, "Could not start transaction!\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        /* Acquire write lock */
        zsdb_write_lock_acquire(db, 0);

        /* Add records */
        for (i = 0; i < ARRAY_SIZE(kvrecs); i++) {
                ret = zsdb_add(db, kvrecs[i].k, kvrecs[i].klen,
                               kvrecs[i].v, kvrecs[i].vlen, &txn);
                if (ret != ZS_OK) {
                        zslog(LOGWARNING, "Failed adding %s to %s\n",
                              (const char *)kvrecs[i].k, DBNAME);
                        ret = EXIT_FAILURE;
                        goto done;
                }
        }

        /* Commit the add records transaction */
        if (zsdb_commit(db, txn) != ZS_OK) {
                zslog(LOGWARNING, "Failed committing transaction!\n");
                ret = EXIT_FAILURE;
                goto done;
        }

done:
        /* Release write lock */
        zsdb_write_lock_release(db);

        /* End the add records transaction */
        zsdb_transaction_end(&txn);

        return ret;
}

int main(int argc _unused_, char **argv _unused_)
{
        struct zsdb *db = NULL;
        int ret = EXIT_SUCCESS;
        struct zsdb_txn *txn = NULL;
        struct ffrock rock;

        if (zsdb_init(&db, NULL, NULL) != ZS_OK) {
                zslog(LOGWARNING, "Failed initialising DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_open(db, DBNAME, MODE_CREATE) != ZS_OK) {
                zslog(LOGWARNING, "Could not create DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        if (add_records(db, txn) != ZS_OK){
                ret = EXIT_FAILURE;
                goto fail1;
        }

        txn = NULL;

        /* Process records */
        rock.db = db;
        rock.state = 0;
        rock.tid = &txn;

        ret = zsdb_foreach(db, NULL, 0, NULL, fe_cb, &rock, rock.tid);
        if (ret != ZS_OK) {
                zslog(LOGWARNING, "Failed running `zsdb_foreach() on %s",
                      DBNAME);
                ret = EXIT_FAILURE;
                goto fail1;
        }
        assert(ret == ZS_OK);
        assert(rock.state == 7);


        /* Commit the transaction */
        zsdb_write_lock_acquire(db, 0);
        if (zsdb_commit(db, txn) != ZS_OK) {
                zslog(LOGWARNING, "Failed committing transaction!\n");
                ret = EXIT_FAILURE;
                goto done;
        }
        zsdb_write_lock_release(db);
        zsdb_transaction_end(&txn);


        ret = EXIT_SUCCESS;
fail1:
        if (zsdb_close(db) != ZS_OK) {
                zslog(LOGWARNING, "Could not close DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

done:
        zsdb_final(&db);

        recursive_rm(DBNAME);

        if (ret == EXIT_SUCCESS)
                printf("foreach() changes: SUCCESS\n");
        else
                printf("foreach() changes: FAILED\n");

        exit(ret);
}
