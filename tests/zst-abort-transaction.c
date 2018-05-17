/*
 * zeroskip
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 * Copyright (c) 2017 Partha Susarla <mail@spartha.org>
 */

#include "zeroskip.h"
#include "macros.h"
#include "log.h"
#include "util.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define DBNAME "ZSTESTDB"

static struct {
        unsigned char *k;
        size_t klen;
        unsigned char *v;
        size_t vlen;
} kvrecs1[] = {
        { (unsigned char *)"123", 3, (unsigned char *)"456", 3 },
        { (unsigned char *)"foo", 3, (unsigned char *)"bar", 3 },
        { (unsigned char *)"abc", 3, (unsigned char *)"def", 3 },
        { (unsigned char *)"abc.name", 8, (unsigned char *)"foo", 3 },
        { (unsigned char *)"1233", 4, (unsigned char *)"456", 3 },
        { (unsigned char *)"abc.place", 9, (unsigned char *)"foo", 3 },
        { (unsigned char *)"1232", 4, (unsigned char *)"456", 3 },
        { (unsigned char *)"abc.animal", 10, (unsigned char *)"foo", 3 },
        { (unsigned char *)"Apple", 5, (unsigned char *)"iPhone7s", 8 },
        { (unsigned char *)"abc.thing", 9, (unsigned char *)"foo", 3 },
        { (unsigned char *)"12311", 5, (unsigned char *)"456", 3 },
        { (unsigned char *)"blackberry", 10, (unsigned char *)"BB10", 3 },
        { (unsigned char *)"1231", 4, (unsigned char *)"456", 3 },
        { (unsigned char *)"nokia", 5, (unsigned char *)"meego", 5 },
};

static struct {
        unsigned char *k;
        size_t klen;
        unsigned char *v;
        size_t vlen;
} kvrecs2[] = {
        { (unsigned char *)"Australia.Sydney", 4, (unsigned char *)"2000", 3 },
        { (unsigned char *)"Australia.Melbourne", 5, (unsigned char *)"3000", 5 },
};

static int record_count = 0;

static int fe_p(void *data _unused_,
                unsigned char *key _unused_, size_t keylen _unused_,
                unsigned char *val _unused_, size_t vallen _unused_)
{
        record_count++;

        return 0;
}

static int fe_cb(void *data _unused_,
                 unsigned char *key, size_t keylen,
                 unsigned char *val, size_t vallen)
{
        size_t i;

        for (i = 0; i < keylen; i++)
                printf("%c", key[i]);

        printf(" : ");

        for (i = 0; i < vallen; i++)
                printf("%c", val[i]);

        printf("\n");

        return 0;
}

static int add_records_to_db_and_commit(struct zsdb *db, struct txn *txn)
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
        for (i = 0; i < ARRAY_SIZE(kvrecs1); i++) {
                ret = zsdb_add(db, kvrecs1[i].k, kvrecs1[i].klen,
                               kvrecs1[i].v, kvrecs1[i].vlen, &txn);
                if (ret != ZS_OK) {
                        zslog(LOGWARNING, "Failed adding %s to %s\n",
                              (char *)kvrecs1[i].k, DBNAME);
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

static int add_two_records_to_db_and_dont_commit(struct zsdb *db,
                                                 struct txn *txn)
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
        for (i = 0; i < ARRAY_SIZE(kvrecs2); i++) {
                ret = zsdb_add(db, kvrecs2[i].k, kvrecs2[i].klen,
                               kvrecs2[i].v, kvrecs2[i].vlen, &txn);
                if (ret != ZS_OK) {
                        zslog(LOGWARNING, "Failed adding %s to %s\n",
                              (char *)kvrecs2[i].k, DBNAME);
                        ret = EXIT_FAILURE;
                        goto done;
                }
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
        struct txn *txn = NULL;

        if (zsdb_init(&db) != ZS_OK) {
                zslog(LOGWARNING, "Failed initialising DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_open(db, DBNAME, MODE_CREATE) != ZS_OK) {
                zslog(LOGWARNING, "Could not create DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        /* Add records and commit */
        if (add_records_to_db_and_commit(db, txn) != ZS_OK){
                ret = EXIT_FAILURE;
                goto fail1;
        }

        txn = NULL;

        /* Count records */
        ret = zsdb_foreach(db, NULL, 0, fe_p, fe_cb, NULL, &txn);
        if (ret != ZS_OK) {
                zslog(LOGWARNING, "Failed running `zsdb_foreach() on %s",
                      DBNAME);
                ret = EXIT_FAILURE;
                goto fail1;
        }
        assert(record_count == ARRAY_SIZE(kvrecs1));

        /* Add more records, but don't commit  */
        if (add_two_records_to_db_and_dont_commit(db, txn) != ZS_OK){
                ret = EXIT_FAILURE;
                goto fail1;
        }

        record_count = 0;
        /* Count records */
        ret = zsdb_foreach(db, NULL, 0, fe_p, fe_cb, NULL, &txn);
        if (ret != ZS_OK) {
                zslog(LOGWARNING, "Failed running `zsdb_foreach() on %s",
                      DBNAME);
                ret = EXIT_FAILURE;
                goto fail1;
        }

        assert(record_count == (ARRAY_SIZE(kvrecs1) + ARRAY_SIZE(kvrecs2)));

        /* Abort transaction */
        if (zsdb_abort(db, &txn) != ZS_NOTIMPLEMENTED) {
                ret = EXIT_FAILURE;
                goto fail1;
        }

        /* Now close the DB and open again, and the db should contain only
         * records from `kvrecs1`.
         */
        if (zsdb_close(db) != ZS_OK) {
                zslog(LOGWARNING, "Could not close DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        zsdb_final(&db);

        if (zsdb_init(&db) != ZS_OK) {
                zslog(LOGWARNING, "Failed initialising DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_open(db, DBNAME, MODE_RDWR) != ZS_OK) {
                zslog(LOGWARNING, "Could not create DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        record_count = 0;
        /* Count records */
        ret = zsdb_foreach(db, NULL, 0, fe_p, fe_cb, NULL, &txn);
        if (ret != ZS_OK) {
                zslog(LOGWARNING, "Failed running `zsdb_foreach() on %s",
                      DBNAME);
                ret = EXIT_FAILURE;
                goto fail1;
        }

        assert(record_count == ARRAY_SIZE(kvrecs1));

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
                printf("abort transaction: SUCCESS\n");
        else
                printf("abort transaction: FAILED\n");

        exit(ret);
}
