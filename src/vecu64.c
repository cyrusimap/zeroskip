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
#include "vecu64.h"

/**
 * Private functions
 */
#define SLICE 64
static void ensure_alloc(struct vecu64 *v, int newsize)
{
        if (newsize <= v->alloc)
                return;
        newsize = ((newsize + (SLICE - 1)) / SLICE) * SLICE;
        v->data = xrealloc(v->data, sizeof(uint64_t) * newsize);
        memset(v->data + v->alloc, 0, sizeof(uint64_t) * (newsize - v->alloc));
        v->alloc = newsize;
}

static inline int adjust_index_ro(const struct vecu64 *v, int idx)
{
        if (idx >= v->count)
                return -1;
        else if (idx < 0)
                idx += v->count;

        return idx;
}

static inline int adjust_index_rw(struct vecu64 *v, int idx, int grow)
{
        if (idx >= v->count)
                ensure_alloc(v, idx + grow);
        else if (idx < 0) {
                idx += v->count;
                if ((idx >= 0) && grow)
                        ensure_alloc(v, v->count + grow);
        } else if (grow) {
                ensure_alloc(v, v->count + grow);
        }

        return idx;
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

int vecu64_append(struct vecu64 *v, uint64_t n)
{
        int pos = v->count++;

        ensure_alloc(v, v->count);
        v->data[pos] = n;
        return pos;
}

void vecu64_insert(struct vecu64 *v, int idx, uint64_t n)
{
        if ((idx = adjust_index_rw(v, idx, 1)) < 0)
                return;
        if (idx < v->count)
                memmove(v->data + idx + 1, v->data + idx,
                        sizeof(uint64_t) * (v->count - idx));
        v->data[idx] = n;
        v->count++;
}

uint64_t vecu64_remove(struct vecu64 *v, int idx)
{
        uint64_t n;

        if ((idx = adjust_index_ro(v, idx)) < 0)
                return 0;
        n = v->data[idx];
        v->count--;

        if (idx < v->count)
                memmove(v->data + idx, v->data + idx + 1,
                        sizeof(uint64_t) * (v->count - idx));
        v->data[v->count] = 0;

        return n;
}

int vecu64_find(struct vecu64 *v, uint64_t n, int idx)
{
        int i;

        if ((idx = adjust_index_ro(v, idx)) < 0)
                return -1;

        for (i = idx; i < v->count; i++) {
                if (v->data[i] == n)
                        return i;
        }

        return -1;
}

int vecu64_size(struct vecu64 *v)
{
        return v->count;
}

int vecu64_foreach(struct vecu64 *v, vecu64_foreach_cb_t cb,
                                 void *cbdata)
{
        int i, idx = 0;

        if ((idx = adjust_index_ro(v, idx)) < 0)
                return 0;

        for (i = idx; i < v->count; i++) {
                cb(cbdata, v->data[i]);
        }

        return 1;
}
