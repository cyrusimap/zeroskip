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
#include <libzeroskip/strarray.h>

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <check.h>

Suite *strarr_suite(void);

static struct str_array sarr;

static void setup(void)
{
        str_array_init(&sarr);
}

static void teardown(void)
{
        str_array_clear(&sarr);
}

START_TEST(test_strarray_create)
{
        ck_assert_int_eq(sarr.alloc, 0);
        ck_assert_int_eq(sarr.count, 0);
}
END_TEST

START_TEST(test_strarray_add)
{
        str_array_add(&sarr, "foo");
        str_array_add(&sarr, "bar");
        str_array_add(&sarr, "foobar");

        ck_assert_int_eq(sarr.count, 3);
}
END_TEST

START_TEST(test_strarray_split)
{
        const char *str = "foo,bar,foobar";
        str_array_from_strsplit(&sarr, str, ',');

        ck_assert_int_eq(sarr.count, 3);
}
END_TEST

Suite *strarr_suite(void)
{
        Suite *s;
        TCase *tc_core;

        s = suite_create("strarray");

        /* core */
        tc_core = tcase_create("core");
        tcase_add_checked_fixture(tc_core, setup, teardown);

        tcase_add_test(tc_core, test_strarray_create);
        tcase_add_test(tc_core, test_strarray_add);
        tcase_add_test(tc_core, test_strarray_split);

        suite_add_tcase(s, tc_core);

        return s;
}
