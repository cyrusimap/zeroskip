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

static int count;

static int print_cb(void *data _unused_, uint64_t num)
{
        count++;
        return 0;
}

int main(int argc _unused_, char **argv _unused_)
{
        int ret = EXIT_SUCCESS;
        struct vecu64 *arr;
        int i;

        arr = vecu64_new();

        vecu64_append(arr, 92834759);

        for (i = 0 ; i < 100; i++) {
                vecu64_append(arr, 4857929 * i);
        }

        vecu64_foreach(arr, print_cb, NULL);

        assert(count == 101);

        vecu64_free(&arr);

        if (ret == EXIT_SUCCESS)
                printf("vecu64: SUCCESS\n");
        else
                printf("vecu64: FAILED\n");


        exit(ret);
}
