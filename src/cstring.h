/*
 * zeroskip
 *
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 * Copyright (c) 2018 Partha Susarla <mail@spartha.org>
 *
 * cstring - String handling library based of git's strbuf implementation.
 */

#ifndef _CSTRING_H_
#define _CSTRING_H_

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <libzeroskip/macros.h>

CPP_GUARD_START

struct _cstring {
        size_t len;
        size_t alloc;
        char *buf;
};

typedef struct _cstring cstring;
extern char cstring_base[];
#define CSTRING_INIT { .len = 0, .alloc = 0, .buf = cstring_base }

/* cstring_init():
 * Initialise the cstring structure.
 *
 */
void cstring_init(cstring *cstr, size_t len);

/* cstring_release():
 * Release the cstring structure and memory.
 */
void cstring_release(cstring *cstr);

/* cstring_detach():
 * The caller needs to free(), the string returned.
 */
char *cstring_detach(cstring *cstr, size_t *len);

/* cstring_attach():
 * Attach a string to a cstring buffer. You should
 * specify the string to attach, the length of string and
 * the amount of allocated memory. The amount should be
 * larger than the string length. This string must be
 * malloc()ed, and after attaching shouldn't be free()d.
 */
void cstring_attach(cstring *cstr, void *buf, size_t len, size_t alloc);

/* cstring_grow():
 * Increase the cstring buffer size allocated length by `len`.
 */
void cstring_grow(cstring *cstr, size_t len);

/* cstring_available() :
 * Returns the available space in the cstring buffer.
 */
static inline size_t cstring_available(const cstring *cstr)
{
        return cstr->alloc ? cstr->alloc - cstr->len - 1 : 0;
}

/* cstring_setlen()
 * Set the length of the cstring buffer to the new value.
 * This function does not allocate new memory.
 */
static inline void cstring_setlen(cstring *cstr, size_t len)
{
        if (len > (cstr->alloc ? cstr->alloc -1 : 0)) {
                fprintf(stderr, "Cannot set length beyond buffer size\n");
                exit(EXIT_FAILURE);
        }
        cstr->len = len;
        if (cstr->buf != cstring_base)
                cstr->buf[len] = '\0';
        else
                assert(!cstring_base[0]);
}
/* cstring_addch():
 * Add a single character to a cstring
 */
static inline void cstring_addch(cstring *cstr, int ch)
{
        if (!cstring_available(cstr))
                cstring_grow(cstr, 1);
        cstr->buf[cstr->len++] = ch;
        cstr->buf[cstr->len] = '\0';
}

/* cstring_add():
 * Add data of a given length to the cstring buffer.
 *
 */
void cstring_add(cstring *cstr, const void *data, size_t len);

/*
 * cstring_addstr():
 * Add a NULL terminated string to the cstring buffer
 */
static inline void cstring_addstr(cstring *cstr, const char *str)
{
        cstring_add(cstr, (const void *)str, strlen(str));
}

/*
 * cstring_dup():
 * Copy the contents of on cstring buffer to another
 */
void cstring_dup(cstring *src, cstring *dest);

/* cstring_ltrim():
 * trim leading spaces from a cstring
 */
void cstring_ltrim(cstring *cstr);

/* cstring_rtrim():
 * trim trailing spaces from a cstring
 */
void cstring_rtrim(cstring *cstr);

/* cstring_trim():
 * trim leading and trailing spaces from a cstring
 */
void cstring_trim(cstring *cstr);

CPP_GUARD_END

#endif  /* _CSTRING_H_ */

