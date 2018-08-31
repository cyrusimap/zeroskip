/*
 * zeroskip
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 * Copyright (c) 2018 Partha Susarla <mail@spartha.org>
 */

#include <libzeroskip/zeroskip.h>
#include <libzeroskip/macros.h>
#include <libzeroskip/log.h>
#include <libzeroskip/util.h>

#include <assert.h>
#include <error.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <check.h>

Suite *zsdb_suite(void);

static struct {
        const unsigned char *k;
        size_t klen;
        const unsigned char *v;
        size_t vlen;
} kvrecs1[] = {
        { (const unsigned char *)"123", 3, (const unsigned char *)"456", 3 },
        { (const unsigned char *)"foo", 3, (const unsigned char *)"bar", 3 },
        { (const unsigned char *)"abc", 3, (const unsigned char *)"def", 3 },
        { (const unsigned char *)"abc.name", 8, (const unsigned char *)"foo", 3 },
        { (const unsigned char *)"1233", 4, (const unsigned char *)"456", 3 },
        { (const unsigned char *)"abc.place", 9, (const unsigned char *)"foo", 3 },
        { (const unsigned char *)"1232", 4, (const unsigned char *)"456", 3 },
        { (const unsigned char *)"abc.animal", 10, (const unsigned char *)"foo", 3 },
        { (const unsigned char *)"Apple", 5, (const unsigned char *)"iPhone7s", 8 },
        { (const unsigned char *)"abc.thing", 9, (const unsigned char *)"foo", 3 },
        { (const unsigned char *)"12311", 5, (const unsigned char *)"456", 3 },
        { (const unsigned char *)"blackberry", 10, (const unsigned char *)"BB10", 3 },
        { (const unsigned char *)"1231", 4, (const unsigned char *)"456", 3 },
        { (const unsigned char *)"nokia", 5, (const unsigned char *)"meego", 5 },
};

static struct {
        const unsigned char *k;
        size_t klen;
        const unsigned char *v;
        size_t vlen;
} kvrecs2[] = {
        { (const unsigned char *)"Australia.Sydney", 4, (const unsigned char *)"2000", 3 },
        { (const unsigned char *)"Australia.Melbourne", 5, (const unsigned char *)"3000", 5 },
};

struct zsdb *db = NULL;
static char *basedir = NULL;

static char *get_basedir(void)
{
        const char *tmpdir;
        char path[PATH_MAX];

        tmpdir = getenv("TMPDIR");
        if (!tmpdir)
                tmpdir = "/tmp";

        snprintf(path, sizeof(path), "%s/zsdb-test.%d", tmpdir, getpid());

        #if 0
        r = xmkdir(path, 0700);
        if (r && errno != EEXIST) {
                perror(path);
                return NULL;
        }
        #endif

        return xstrdup(path);
}

static void setup(void)
{
        int ret;

        basedir = get_basedir();

        ret = zsdb_init(&db, NULL, NULL);
        ck_assert_int_eq(ret, ZS_OK);

        ret = zsdb_open(db, basedir, MODE_CREATE);
        ck_assert_int_eq(ret, ZS_OK);
}

static void teardown(void)
{
        int ret;
        ret = zsdb_close(db);
        ck_assert_int_eq(ret, ZS_OK);
        zsdb_final(&db);
        recursive_rm(basedir);
        free(basedir);
}

static int record_count = 0;

static int count_fe_p(void *data _unused_,
                      const unsigned char *key _unused_, size_t keylen _unused_,
                      const unsigned char *val _unused_, size_t vallen _unused_)
{
        record_count++;

        return 0;
}

START_TEST(test_abort_transaction)
{
        struct zsdb_txn *txn;
        size_t i;
        int ret;

        txn = NULL;
        record_count = 0;

        /* Begin transaction */
        ret = zsdb_transaction_begin(db, &txn);
        ck_assert_int_eq(ret, ZS_OK);

        /* Acquire write lock */
        zsdb_write_lock_acquire(db, 0);

        for (i = 0; i < ARRAY_SIZE(kvrecs1); i++) {
                ret = zsdb_add(db, kvrecs1[i].k, kvrecs1[i].klen,
                               kvrecs1[i].v, kvrecs1[i].vlen, &txn);
                ck_assert_int_eq(ret, ZS_OK);
        }

        /* Commit the add records transaction */
        zsdb_commit(db, &txn);
        ck_assert_int_eq(ret, ZS_OK);

        /* Release write lock */
        zsdb_write_lock_release(db);

        zsdb_transaction_end(&txn);

        /* Count records */
        ret = zsdb_foreach(db, NULL, 0, count_fe_p, NULL, NULL, &txn);
        ck_assert_int_eq(ret, ZS_OK);
        ck_assert_int_eq(record_count, ARRAY_SIZE(kvrecs1));

        /* Add more records, but don't commit  */
        /* Begin transaction */
        ret = zsdb_transaction_begin(db, &txn);
        ck_assert_int_eq(ret, ZS_OK);

        /* Acquire write lock */
        zsdb_write_lock_acquire(db, 0);

        for (i = 0; i < ARRAY_SIZE(kvrecs2); i++) {
                ret = zsdb_add(db, kvrecs2[i].k, kvrecs2[i].klen,
                               kvrecs2[i].v, kvrecs2[i].vlen, &txn);
                ck_assert_int_eq(ret, ZS_OK);
        }

        /* Release write lock */
        zsdb_write_lock_release(db);

        /* End transaction */
        zsdb_transaction_end(&txn);

        /* Count records, again */
        record_count = 0;
        ret = zsdb_foreach(db, NULL, 0, count_fe_p, NULL, NULL, &txn);
        ck_assert_int_eq(ret, ZS_OK);
        ck_assert_int_eq(record_count, (ARRAY_SIZE(kvrecs1) + ARRAY_SIZE(kvrecs2)));

        /* Abort transaction */
        ret = zsdb_abort(db, &txn);
        ck_assert_int_eq(ret, ZS_OK);

        /* Now close the DB and open again, and the db should contain only
         * records from `kvrecs1`.
         */
        ret = zsdb_close(db);
        ck_assert_int_eq(ret, ZS_OK);
        zsdb_final(&db);

        ret = zsdb_init(&db, NULL, NULL);
        ck_assert_int_eq(ret, ZS_OK);

        ret = zsdb_open(db, basedir, MODE_CREATE);
        ck_assert_int_eq(ret, ZS_OK);

        /* Count records */
        record_count = 0;
        ret = zsdb_foreach(db, NULL, 0, count_fe_p, NULL, NULL, &txn);
        ck_assert_int_eq(ret, ZS_OK);
        ck_assert_int_eq(record_count, ARRAY_SIZE(kvrecs1));
}
END_TEST

START_TEST(test_many_records)
{
        struct zsdb_txn *txn;
        size_t i, NUM_RECS;
        int ret;

        NUM_RECS = 4096;

        txn = NULL;

        /* Begin transaction */
        ret = zsdb_transaction_begin(db, &txn);
        ck_assert_int_eq(ret, ZS_OK);

        /* Acquire write lock */
        zsdb_write_lock_acquire(db, 0);

        /* Add 4096 records */
        for (i = 0; i < NUM_RECS; i++) {
                unsigned char key[24], val[24];

                snprintf((char *)key, 8, "key%zu", i);
                snprintf((char *)val, 8, "val%zu", i);

                ret = zsdb_add(db, key, strlen((char *)key), val,
                               strlen((char *)val), &txn);
                ck_assert_int_eq(ret, ZS_OK);
        }

        /* Commit the add records transaction */
        zsdb_commit(db, &txn);
        ck_assert_int_eq(ret, ZS_OK);

        /* Release write lock */
        zsdb_write_lock_release(db);

        zsdb_transaction_end(&txn);

        /* Count records */
        record_count = 0;
        txn = NULL;
        ret = zsdb_foreach(db, NULL, 0, count_fe_p, NULL, NULL, &txn);
        ck_assert_int_eq(ret, ZS_OK);
        ck_assert_int_eq(record_count, NUM_RECS);
}
END_TEST

Suite *zsdb_suite(void)
{
        Suite *s;
        TCase *tc_abort;
        TCase *tc_many;

        s = suite_create("zeroskip");

        /* abort */
        tc_abort = tcase_create("abort");
        tcase_add_checked_fixture(tc_abort, setup, teardown);

        tcase_add_test(tc_abort, test_abort_transaction);
        suite_add_tcase(s, tc_abort);

        /* many records */
        tc_many = tcase_create("many");
        tcase_add_checked_fixture(tc_many, setup, teardown);

        tcase_add_test(tc_many, test_many_records);
        suite_add_tcase(s, tc_many);

        return s;
}
