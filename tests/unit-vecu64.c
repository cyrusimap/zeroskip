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

static void setup(void)
{
}

static void teardown(void)
{
}

Suite *vecu64_suite(void)
{
        Suite *s;

        s = suite_create("vecu64");

        return s;

}
