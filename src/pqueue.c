/*
 * pqueue.c
 *
 * A priority queue implementation
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#include "pqueue.h"
#include "util.h"

#include <string.h>

/**
 * Private functions
 */
static inline int cmp(struct pqueue *pq, int i, int j)
{
        return pq->cmp(pq->arr[i].data, pq->arr[j].data, pq->cmpfndata);
}

static inline void swp(struct pqueue *pq, int i, int j)
{
        unsigned char tmp[sizeof(struct pqueue_data)];

        memcpy(tmp, &pq->arr[i], sizeof(pq->arr[i]));
        memcpy(&pq->arr[i], &pq->arr[j], sizeof(pq->arr[i]));
        memcpy(&pq->arr[j], tmp, sizeof(pq->arr[i]));
}


/**
 * Public functions
 */
void pqueue_put(struct pqueue *pq, void *data)
{
        int i, j;

        ALLOC_GROW(pq->arr, pq->count + 1, pq->alloc);
        pq->arr[pq->count].data = data;
        pq->count++;

        for (i = pq->count - 1; i; i = j) {
                j = (i - 1) / 2;
                if (cmp(pq, j, i) <= 0)
                        break;

                swp(pq, j, i);
        }
}

/* pqueue_get():
 * returns the top of the queue. User needs to free data returned.
 */
void *pqueue_get(struct pqueue *pq)
{
        void *data;
        int i, j;

        if (!pq->count)
                return NULL;

        data = pq->arr[0].data;

        if (!--pq->count)
                return data;

        pq->arr[0] = pq->arr[pq->count];
        for (i = 0; i * 2 + 1 < pq->count; i = j) {
                j = i * 2 + 1;

                if ((j + 1 < pq->count) && (cmp(pq, j, j + 1) >= 0))
                        j++;

                if (cmp(pq, i, j) <= 0)
                        break;

                swp(pq, j, i);
        }

        return data;
}

/* pqueue_peek():
 * Get a peek of the top of the queue, doesn't change the PQ.
 */
void *pqueue_peek(struct pqueue *pq)
{
        if (!pq->count)
                return NULL;

        return pq->arr[0].data;
}

void pqueue_free(struct pqueue *pq)
{
        xfree(pq->arr);
        pq->count = 0;
        pq->alloc = 0;
}
