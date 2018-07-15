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

static struct {
        const unsigned char *k;
        size_t klen;
        const unsigned char *v;
        size_t vlen;
} kvrecs[] = {
        { (const unsigned char *)"buzzes", 6, (const unsigned char *)"afro timur funky cents hewitt", 29 },
        { (const unsigned char *)"galas", 5, (const unsigned char *)"assad goering flemish brynner heshvan", 37 },
        { (const unsigned char *)"bathes", 6, (const unsigned char *)"flax corm naipaul enable herrera fating", 39 },
};

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

        return ret;
}


static int delete_record(struct zsdb *db, struct zsdb_txn *txn _unused_)
{
        int ret = ZS_OK;

        /* Acquire write lock */
        zsdb_write_lock_acquire(db, 0);

        /* Delete a record */
        if (zsdb_remove(db, kvrecs[1].k, kvrecs[1].klen, &txn) != ZS_OK) {
                zslog(LOGWARNING, "Failed removing record `foo`!\n");
                ret = EXIT_FAILURE;
                goto done;
        }
done:
        /* Release write lock */
        zsdb_write_lock_release(db);

        return ret;
}

int main(int argc _unused_, char **argv _unused_)
{
        struct zsdb *db = NULL;
        int ret = EXIT_SUCCESS;
        struct zsdb_txn *txn = NULL;
        const unsigned char *value = NULL;
        size_t vallen = 0;

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

        if (add_records(db, txn) != ZS_OK){
                ret = EXIT_FAILURE;
                goto fail1;
        }


       if (delete_record(db, txn) != ZS_OK) {
                ret = EXIT_FAILURE;
                goto fail1;
        }

        /* Fetch key2  - should not be found*/
        ret = zsdb_fetch(db, kvrecs[1].k, kvrecs[1].klen,
                         &value, &vallen, &txn);
        assert(ret == ZS_NOTFOUND);

        /* Fetch key1 and key3, they should be found */
        ret = zsdb_fetch(db, kvrecs[0].k, kvrecs[0].klen,
                         &value, &vallen, &txn);
        assert(ret == ZS_OK);

        ret = zsdb_fetch(db, kvrecs[2].k, kvrecs[2].klen,
                         &value, &vallen, &txn);
        assert(ret == ZS_OK);

        /* Commit */
        if (zsdb_commit(db, txn) != ZS_OK) {
                zslog(LOGWARNING, "Failed committing transaction!\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        /* Fetch key2  - should not be found*/
        ret = zsdb_fetch(db, kvrecs[1].k, kvrecs[1].klen,
                         &value, &vallen, &txn);
        assert(ret == ZS_NOTFOUND);

        /* Fetch key1 and key3, they should be found */
        ret = zsdb_fetch(db, kvrecs[0].k, kvrecs[0].klen,
                         &value, &vallen, &txn);
        assert(ret == ZS_OK);

        ret = zsdb_fetch(db, kvrecs[2].k, kvrecs[2].klen,
                         &value, &vallen, &txn);
        assert(ret == ZS_OK);


        /* Close and reopen DB */
        zsdb_close(db);
        zsdb_final(&db);
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


        /* Fetch key2  - should not be found*/
        ret = zsdb_fetch(db, kvrecs[1].k, kvrecs[1].klen,
                         &value, &vallen, &txn);
        assert(ret == ZS_NOTFOUND);

        /* Fetch key1 and key3, they should be found */
        ret = zsdb_fetch(db, kvrecs[0].k, kvrecs[0].klen,
                         &value, &vallen, &txn);
        assert(ret == ZS_OK);

        ret = zsdb_fetch(db, kvrecs[2].k, kvrecs[2].klen,
                         &value, &vallen, &txn);
        assert(ret == ZS_OK);


        /* Everything is good */
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
                printf("delete: SUCCESS\n");
        else
                printf("delete: FAILED\n");

        exit(ret);
}
