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
#include <libzeroskip/vecu64.h>

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <check.h>

Suite *vecu64_suite(void);

static int count;
static struct vecu64 *arr;

static void setup(void)
{
        arr = vecu64_new();
}

static void teardown(void)
{
        vecu64_free(&arr);
}

START_TEST(test_vecu64_create)
{
        ck_assert_int_eq(arr->alloc, 0);
        ck_assert_int_eq(arr->count, 0);
}
END_TEST                        /* test_vecu64_create */

static int print_cb(void *data _unused_, uint64_t num)
{
        count++;
        return 0;
}

START_TEST(test_vecu64_foreach)
{
        int i;

        vecu64_append(arr, 92834759);

        for (i = 0 ; i < 100; i++) {
                vecu64_append(arr, 4857929 * i);
        }

        vecu64_foreach(arr, print_cb, NULL);

        ck_assert_int_eq(arr->count, 101);
        ck_assert_int_eq(count, 101);
}
END_TEST                        /* test_vecu64_foreach */


Suite *vecu64_suite(void)
{
        Suite *s;
        TCase *tc_core;
        TCase *tc_foreach;

        s = suite_create("vecu64");

        /* core */
        tc_core = tcase_create("core");
        tcase_add_checked_fixture(tc_core, setup, teardown);
        tcase_add_test(tc_core, test_vecu64_create);
        suite_add_tcase(s, tc_core);

        /* foreach */
        tc_foreach = tcase_create("foreach");
        tcase_add_checked_fixture(tc_foreach, setup, teardown);
        tcase_add_test(tc_foreach, test_vecu64_foreach);
        suite_add_tcase(s, tc_foreach);


        return s;
}
