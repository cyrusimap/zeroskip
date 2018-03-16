/*
 * vecu64.h
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#ifndef _VECU64_H_
#define _VECU64_H_

#include <stdint.h>
#include <string.h>

struct vecu64 {
        int count;
        int alloc;
        uint64_t *data;
};

#define VECU64_INIT { 0, 0, NULL }

struct vecu64 *vecu64_new(void);
void vecu64_free(struct vecu64 **v);
int vecu64_append(struct vecu64 *v, uint64_t n);
void vecu64_insert(struct vecu64 *v, int idx, uint64_t n);
uint64_t vecu64_remove(struct vecu64 *v, int idx);
int vecu64_find(struct vecu64 *v, uint64_t n, int idx);
int vecu64_size(struct vecu64 *v);

typedef int (*vecu64_foreach_cb_t)(void *data, uint64_t offset);
int vecu64_foreach(struct vecu64 *v, vecu64_foreach_cb_t cb,
                   void *cbdata);


#endif  /* _VECU64_H_ */
