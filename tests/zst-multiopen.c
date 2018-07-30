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
        { (const unsigned char *)"mustache", 8, (const unsigned char *)"blog lomo", 9 },
        { (const unsigned char *)"cred", 4, (const unsigned char *)"beard ethical", 13 },
        { (const unsigned char *)"leggings", 8, (const unsigned char *)"tumblr salvia", 13 },
};

static int record_count = 0;

static int fe_p(void *data _unused_,
                const unsigned char *key _unused_, size_t keylen _unused_,
                const unsigned char *val _unused_, size_t vallen _unused_)
{
        record_count++;

        return 0;
}

static int fe_cb(void *data _unused_,
                 const unsigned char *key, size_t keylen,
                 const unsigned char *val, size_t vallen)
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


int main(int argc _unused_, char **argv _unused_)
{
        struct zsdb *db1 = NULL;
        struct zsdb *db2 = NULL;
        int ret = EXIT_SUCCESS;
        struct zsdb_txn *txn = NULL;

        /* Open a new zeroskip DB, this is the first instance of the DB */
        if (zsdb_init(&db1, NULL) != ZS_OK) {
                zslog(LOGWARNING, "Failed initialising DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_open(db1, DBNAME, MODE_CREATE) != ZS_OK) {
                zslog(LOGWARNING, "Could not create DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        /* Add a record to the DB */
        zsdb_write_lock_acquire(db1, 0);

        ret = zsdb_add(db1, kvrecs[0].k, kvrecs[0].klen,
                       kvrecs[0].v, kvrecs[0].vlen,
                       NULL);
        if (ret != ZS_OK) {
                zslog(LOGWARNING, "Failed adding %s to %s\n",
                      (const char *)kvrecs[0].k, DBNAME);
                ret = EXIT_FAILURE;
                goto fail1;
        }

        if (zsdb_commit(db1, NULL) != ZS_OK) {
                zslog(LOGWARNING, "Failed committing transaction!\n");
                ret = EXIT_FAILURE;
                goto done;
        }
        zsdb_write_lock_release(db1);


        /* Open the DB again, this is the second instance of the DB */
        if (zsdb_init(&db2, NULL) != ZS_OK) {
                zslog(LOGWARNING, "Failed initialising DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_open(db2, DBNAME, MODE_RDWR) != ZS_OK) {
                zslog(LOGWARNING, "Could not open DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        /* Add a record to the DB */
        zsdb_write_lock_acquire(db2, 0);

        ret = zsdb_add(db2, kvrecs[1].k, kvrecs[1].klen,
                       kvrecs[1].v, kvrecs[1].vlen,
                       NULL);
        if (ret != ZS_OK) {
                zslog(LOGWARNING, "Failed adding %s to %s\n",
                      (const char *)kvrecs[1].k, DBNAME);
                ret = EXIT_FAILURE;
                goto fail1;
        }

        if (zsdb_commit(db2, NULL) != ZS_OK) {
                zslog(LOGWARNING, "Failed committing transaction!\n");
                ret = EXIT_FAILURE;
                goto done;
        }
        zsdb_write_lock_release(db2);

        /* Close the second instance of the DB */
        if (zsdb_close(db2) != ZS_OK) {
                zslog(LOGWARNING, "Could not close DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        zsdb_final(&db2);

        /* The 1st DB instance should still be open and should work */
        /* Add a record */
        zsdb_write_lock_acquire(db1, 0);

        ret = zsdb_add(db1, kvrecs[2].k, kvrecs[2].klen,
                       kvrecs[2].v, kvrecs[2].vlen,
                       NULL);
        if (ret != ZS_OK) {
                zslog(LOGWARNING, "Failed adding %s to %s\n",
                      (const char *)kvrecs[2].k, DBNAME);
                ret = EXIT_FAILURE;
                goto fail1;
        }

        if (zsdb_commit(db1, NULL) != ZS_OK) {
                zslog(LOGWARNING, "Failed committing transaction!\n");
                ret = EXIT_FAILURE;
                goto done;
        }
        zsdb_write_lock_release(db1);

        /* Close the first instance of the DB */
        if (zsdb_close(db1) != ZS_OK) {
                zslog(LOGWARNING, "Could not close DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        zsdb_final(&db1);

        ret = EXIT_SUCCESS;

        /* Count records */
        txn = NULL;

        if (zsdb_init(&db1, NULL) != ZS_OK) {
                zslog(LOGWARNING, "Failed initialising DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_open(db1, DBNAME, MODE_CREATE) != ZS_OK) {
                zslog(LOGWARNING, "Could not create DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        ret = zsdb_foreach(db1, NULL, 0, fe_p, fe_cb, NULL, &txn);
        if (ret != ZS_OK) {
                zslog(LOGWARNING, "Failed running `zsdb_foreach() on %s",
                      DBNAME);
                ret = EXIT_FAILURE;
                goto fail1;
        }

        assert(record_count == (ARRAY_SIZE(kvrecs)));

fail1:
        if (zsdb_close(db1) != ZS_OK) {
                zslog(LOGWARNING, "Could not close DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

done:
        zsdb_final(&db1);

        recursive_rm(DBNAME);

        if (ret == EXIT_SUCCESS)
                printf("multiopen: SUCCESS\n");
        else
                printf("multiopen: FAILED\n");

        exit(ret);
}
