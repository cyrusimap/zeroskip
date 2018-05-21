/*
 * pqueue.h
 *
 * A priority queue implementation
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */
#ifndef _PQUEUE_H_
#define _PQUEUE_H_

#include <stdio.h>

#include <libzeroskip/macros.h>

CPP_GUARD_START

typedef int (*pqueue_cmp_fn)(const void *d1, const void *d2, void *cbdata);

struct pqueue_data {
        void *data;
};

struct pqueue {
        pqueue_cmp_fn cmp;
        void *cmpfndata;
        int alloc;
        int count;
        struct pqueue_data *arr;
};

void pqueue_put(struct pqueue *pq, void *data);
void *pqueue_get(struct pqueue *pq);
void *pqueue_peek(struct pqueue *pq);
void pqueue_free(struct pqueue *pq);

CPP_GUARD_END
#endif  /* _PQUEUE_H_ */
