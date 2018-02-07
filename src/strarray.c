/*
 * strarray.c
 *
 * An array of strings
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */
#include "util.h"
#include "strarray.h"

#include <string.h>

const char *null_data[] = { NULL };

void str_array_init(struct str_array *arr)
{
        arr->datav = null_data;
        arr->count = 0;
        arr->alloc = 0;
}

void str_array_clear(struct str_array *arr)
{
        if (arr->datav != null_data) {
                int i;
                for (i = 0; i < arr->count; i++) {
                        char *data = (char *)arr->datav[i];
                        xfree(data);
                }
                xfree(arr->datav);
        }

        str_array_init(arr);
}




/* str_array_add():
 * Adds a string to the end of the array.
 */
void str_array_add(struct str_array *arr, const char *str)
{
        if (arr->datav == null_data)
                arr->datav = NULL;

        ALLOC_GROW(arr->datav, arr->count + 2, arr->alloc);
        arr->datav[arr->count++] = xstrdup(str);
        arr->datav[arr->count] = NULL;
}

/* str_array_addv():
 */
void str_array_addv(struct str_array *arr, const char **argv)
{
        for (; *argv; argv++)
                str_array_add(arr, *argv);
}

/* str_array_remove():
 * Removes a string from the end end of the array
 * (the latest entry in the array)
 */
void str_array_remove(struct str_array *arr)
{
        char *data;

        if (!arr->count)
                return;
        data = (char *)arr->datav[arr->count - 1];
        xfree(data);
        arr->datav[arr->count - 1] = NULL;
        arr->count--;
}

/* str_array_from_strsplit():
 * Creates an str_array by splitting a string by some delimiter.
 */
void str_array_from_strsplit(struct str_array *arr, const char *str,
                             size_t slen, char delim)
{
        size_t len = slen;
        const char *p = str;

        str_array_clear(arr);

        while (len) {
                int tlen = len;
                const char *end = memchr(str, delim, len);
                char *s;
                if (end)
                        tlen = end - str + 1;

                s = xmalloc(sizeof(char) * tlen);
                memset(s, 0, tlen);
                memcpy(s, p, tlen - 1);
                str_array_add(arr, s);
                xfree(s);

                p += tlen;
                len -= tlen;
        }
}

/* str_array_detach():
 * Returns the data, caller needs to free.
 */
const char **str_array_detach(struct str_array *arr)
{
        if (arr->datav == null_data)
                return xcalloc(1, sizeof(const char *));
        else {
                const char **ret = arr->datav;
                str_array_init(arr);
                return ret;
        }
}
