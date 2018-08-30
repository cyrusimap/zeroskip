/*
 * zeroskip
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 * Copyright (c) 2018 Partha Susarla <mail@spartha.org>
 */
#include <libzeroskip/macros.h>
#include <libzeroskip/util.h>
#include <libzeroskip/btree.h>

#include <check.h>

#define NUMRECS 20

Suite *memtree_suite(void);

static struct btree *tree = NULL;

static void setup(void)
{
        tree = btree_new(NULL, NULL);
}

static void teardown(void)
{
        btree_free(tree);
}

START_TEST(test_memtree_create)
{
        ck_assert_int_eq(tree->count, 0);
}
END_TEST                        /* test_memtree_create */

START_TEST(test_memtree_insert_records)
{
        struct record *recs[NUMRECS];
        int i, ret;

        for (i = 0; i < NUMRECS; i++) {
                char key[10], val[10];

                sprintf(key, "key%d", i);
                sprintf(val, "val%d", i);

                recs[i] = record_new((const unsigned char *)key, strlen(key),
                                     (const unsigned char *)val, strlen(val),
                                     0);
                ret = btree_insert(tree, recs[i]);
                ck_assert_int_eq(ret, BTREE_OK);
        }

        ck_assert_int_eq(tree->count, NUMRECS);
}
END_TEST                        /* test_memtree_insert_records */

START_TEST(test_memtree_insert_duplicate_record)
{
        struct record *recs[NUMRECS];
        struct record *trec;
        int i, ret;
        btree_iter_t iter;

        for (i = 0; i < NUMRECS; i++) {
                char key[10], val[10];

                sprintf(key, "key%d", i);
                sprintf(val, "val%d", i);

                recs[i] = record_new((const unsigned char *)key, strlen(key),
                                     (const unsigned char *)val, strlen(val),
                                     0);
                ret = btree_insert(tree, recs[i]);
                ck_assert_int_eq(ret, BTREE_OK);
        }

        ck_assert_int_eq(tree->count, NUMRECS);

        /* Insert a duplicate record */
        trec = record_new((const unsigned char *)"key2",
                          strlen("key2"),
                          (const unsigned char *)"newval2",
                          strlen("newval2"),
                          0);

        ret = btree_insert(tree, trec);
        ck_assert_int_eq(ret, BTREE_DUPLICATE);

        /* Replace the record */
        ret = btree_replace(tree, trec);
        ck_assert_int_eq(ret, BTREE_OK);

        ck_assert_int_eq(tree->count, NUMRECS);

        /* Ensure value is is the new value */
        memset(&iter, 0, sizeof(btree_iter_t));
        ret = btree_find(tree, (const unsigned char *)"key2",
                         strlen("key2"), iter);
        ck_assert_int_eq(ret, 1);

        ret = memcmp("newval2", iter->record->val, iter->record->vallen);
        ck_assert_int_eq(ret, 0);
}
END_TEST                        /* test_memtree_insert_duplicate_record */

#define MBK1 "INBOX.a b"
#define MBK1L 9
#define MBV1 "val1"

#define MBK2 "INBOX.a.b"
#define MBK2L 9
#define MBV2 "val2"

#define MBK3 "INBOX.a.b.c"
#define MBK3L 11
#define MBV3 "val3"

#define MBK4 "INBOX.a"
#define MBK4L 7
#define MBV4 "val4"

#define MBK5 "INBOX b"
#define MBK5L 7
#define MBV5 "val5"

#define VL 4

struct mboxkvrec {
        const unsigned char *k;
        size_t klen;
        const unsigned char *v;
        size_t vlen;
} mbkv[] = {
        { (const unsigned char *)MBK1, MBK1L, (const unsigned char *)MBV1, VL },
        { (const unsigned char *)MBK2, MBK2L, (const unsigned char *)MBV1, VL },
        { (const unsigned char *)MBK3, MBK3L, (const unsigned char *)MBV1, VL },
        { (const unsigned char *)MBK4, MBK4L, (const unsigned char *)MBV1, VL },
        { (const unsigned char *)MBK5, MBK5L, (const unsigned char *)MBV1, VL },
};

#if CHECK_MINOR_VERSION < 11
#define ck_assert_mem_eq(a,b,c) assert(0 == memcmp(a,b,c))
#endif

START_TEST(test_memtree_mbox_name)
{
        size_t i;
        int ret;
        btree_iter_t iter;

        /* Add records */
        for (i = 0; i < ARRAY_SIZE(mbkv); i++) {
                struct record *trec = record_new(mbkv[i].k, mbkv[i].klen,
                                                 mbkv[i].v, mbkv[i].vlen, 0);
                ret = btree_insert(tree, trec);
                ck_assert_int_eq(ret, BTREE_OK);
        }

        ck_assert_int_eq(tree->count, ARRAY_SIZE(mbkv));

        i = 0;
        memset(&iter, 0, sizeof(btree_iter_t));
        for (btree_begin(tree, iter); btree_next(iter);) {

                if (i == 0)
                        ck_assert_mem_eq(iter->record->key, MBK5, MBK5L);
                if (i == 1)
                        ck_assert_mem_eq(iter->record->key, MBK4, MBK4L);
                if (i == 2)
                        ck_assert_mem_eq(iter->record->key, MBK1, MBK1L);
                if (i == 3)
                        ck_assert_mem_eq(iter->record->key, MBK2, MBK2L);
                if (i == 4)
                        ck_assert_mem_eq(iter->record->key, MBK3, MBK3L);

                i++;
        }
}
END_TEST                        /* test_memtree_mbox_name */

START_TEST(test_memtree_iter)
{
        struct record *recs[NUMRECS];
        int i, ret, count = 0;
        btree_iter_t iter;

        for (i = 0; i < NUMRECS; i++) {
                char key[10], val[10];

                sprintf(key, "key%d", i);
                sprintf(val, "val%d", i);

                recs[i] = record_new((const unsigned char *)key, strlen(key),
                                     (const unsigned char *)val, strlen(val),
                                     0);
                ret = btree_insert(tree, recs[i]);
                ck_assert_int_eq(ret, BTREE_OK);
        }

        ck_assert_int_eq(tree->count, NUMRECS);

        memset(&iter, 0, sizeof(btree_iter_t));
        for (btree_begin(tree, iter); btree_next(iter);) {
                count++;
        }
        ck_assert_int_eq(count, NUMRECS);
}
END_TEST                        /* test_memtree_iter */

Suite *memtree_suite(void)
{
        Suite *s;
        TCase *tc_core;
        TCase *tc_mbox_name_format;
        TCase *tc_iter;

        s = suite_create("memtree");

        /* core */
        tc_core = tcase_create("core");
        tcase_add_checked_fixture(tc_core, setup, teardown);

        tcase_add_test(tc_core, test_memtree_create);
        tcase_add_test(tc_core, test_memtree_insert_records);
        tcase_add_test(tc_core, test_memtree_insert_duplicate_record);

        suite_add_tcase(s, tc_core);

        /* mbox name format test */
        tc_mbox_name_format = tcase_create("mbox_name_format");
        tcase_add_checked_fixture(tc_mbox_name_format, setup, teardown);

        tcase_add_test(tc_mbox_name_format, test_memtree_mbox_name);

        suite_add_tcase(s, tc_mbox_name_format);

        /* test iterators */
        tc_iter = tcase_create("iter");
        tcase_add_checked_fixture(tc_iter, setup, teardown);

        tcase_add_test(tc_iter, test_memtree_iter);

        suite_add_tcase(s, tc_iter);

        return s;

}
