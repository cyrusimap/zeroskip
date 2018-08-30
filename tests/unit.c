/*
 * zeroskip
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 * Copyright (c) 2018 Partha Susarla <mail@spartha.org>
 */

#include <config.h>
#include <stdlib.h>
#include <stdint.h>
#include <check.h>

#include "unit.h"

int main(int argc, char **argv)
{
        SRunner *sr;
        int num_failed;

        sr = srunner_create(zsdb_suite());
        srunner_add_suite(sr, vecu64_suite());
        srunner_add_suite(sr, memtree_suite());

        srunner_run_all(sr, CK_NORMAL);
        num_failed = srunner_ntests_failed(sr);
        srunner_free(sr);

        return (num_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
