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
#include <libzeroskip/crc32c.h>

#include <check.h>
#include <string.h>

Suite *crc32c_suite(void);

static void setup(void)
{
        crc32c_init();
}

static void teardown(void)
{
}

START_TEST(test_crc32c)
{
        const char *s1 = "lorem";
        size_t len1 = 5;
        const char *s2 = " ipsum";
        size_t len2 = 6;
        const char *s3 = "lorem ipsum";

        static uint32_t CRC32C = 0xdfb4e6c9;
        uint32_t c1 = crc32c(0, 0, 0);
        uint32_t c2 = crc32c(0, 0, 0);

        c1 = crc32c(c1, s1, len1);
        c1 = crc32c(c1, s2, len2);

        c2 = crc32c(c2, s3, strlen(s3));

        ck_assert_uint_eq(c1, CRC32C);

        ck_assert_uint_eq(c2, CRC32C);
}
END_TEST                        /* test_crc32c */

Suite *crc32c_suite(void)
{
        Suite *s;
        TCase *tc_core;

        s = suite_create("crc32c");

        /* core crc32c tests */
        tc_core = tcase_create("core");
        tcase_add_checked_fixture(tc_core, setup, teardown);
        tcase_add_test(tc_core, test_crc32c);
        suite_add_tcase(s, tc_core);

        return s;
}
