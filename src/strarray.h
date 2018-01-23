/*
 * strarray.h
 *
 * An array of strings
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#ifndef _STRARRAY_H_
#define _STRARRAY_H_

#include <stdio.h>

struct str_array {
    const char **datav;
    int count;
    int alloc;
};

extern const char *null_data[];

#define STR_ARRAY_INIT { null_data, 0, 0}

void str_array_init(struct str_array *arr);
void str_array_clear(struct str_array *arr);

void str_array_add(struct str_array *arr, const char *str);
void str_array_addv(struct str_array *arr, const char **argv);
void str_array_remove(struct str_array *arr);

void str_array_from_strsplit(struct str_array *arr, const char *str,
                             size_t slen, char delim);

const char **str_array_detach(struct str_array *arr);

#endif  /* _STRARRAY_H_ */
