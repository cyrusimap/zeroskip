/*
 * vecu64.h
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#include <libzeroskip/log.h>
#include <libzeroskip/util.h>
#include <libzeroskip/vecu64.h>

/**
 * Private functions
 */
static void ensure_alloc(struct vecu64 *v, uint64_t newsize)
{
        uint64_t alloced = v->alloc;

        ALLOC_GROW(v->data, newsize, v->alloc);
        memset(v->data + alloced, 0, sizeof(uint64_t) * (newsize - alloced));
}

/**
 * Public functions
 */
struct vecu64 *vecu64_new(void)
{
        struct vecu64 *v;

        v = xmalloc(sizeof(struct vecu64));
        memset(v, 0, sizeof(struct vecu64));

        return v;
}

void vecu64_free(struct vecu64 **vptr)
{
        struct vecu64 *v;
        if (!vptr || !*vptr)
                return;

        v = *vptr;
        *vptr = NULL;

        xfree(v->data);
        xfree(v);
}

uint64_t vecu64_append(struct vecu64 *v, uint64_t n)
{
        uint64_t pos;

        if (unsigned_add_overflows(v->count, 1)) {
                fprintf(stderr, "vecu64: Cannot allocate more memory\n");
                exit(EXIT_FAILURE);
        }

        ensure_alloc(v, v->count + 1);
        pos = v->count + 1;
        v->data[pos] = n;

        return pos;
}

void vecu64_insert(struct vecu64 *v, uint64_t idx, uint64_t n)
{
        if (unsigned_add_overflows(v->count, idx)) {
                fprintf(stderr, "vecu64: Cannot allocate more memory\n");
                exit(EXIT_FAILURE);
        }

        ensure_alloc(v, v->count + idx);

        if (idx < v->count)
                memmove(v->data + idx + 1, v->data + idx,
                        sizeof(uint64_t) * (v->count - idx));

        v->data[idx] = n;
        v->count++;
}

uint64_t vecu64_remove(struct vecu64 *v, uint64_t idx)
{
        uint64_t n;

        if (idx >= v->count)
                return 0;

        n = v->data[idx];
        v->count--;

        if (idx < v->count)
                memmove(v->data + idx, v->data + idx + 1,
                        sizeof(uint64_t) * (v->count - idx));
        v->data[v->count] = 0;

        return n;
}

uint64_t vecu64_find(struct vecu64 *v, uint64_t n, uint64_t idx)
{
        uint64_t i;

        if (idx >= v->count)
                return 0;

        for (i = idx; i < v->count; i++) {
                if (v->data[i] == n)
                        return i;
        }

        return -1;
}

uint64_t vecu64_size(struct vecu64 *v)
{
        return v->count;
}

int vecu64_foreach(struct vecu64 *v, vecu64_foreach_cb_t cb,
                                 void *cbdata)
{
        uint64_t i, idx = 0;

        if (idx >= v->count)
                return 0;

        for (i = idx; i < v->count; i++) {
                cb(cbdata, v->data[i]);
        }

        return 1;
}
