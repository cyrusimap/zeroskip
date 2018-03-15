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
#define vecu64_init(x) (memset((x), 0, sizeof(struct vecu64)))
void vecu64_fini(struct vecu64 *v);

struct vecu64 *vecu64_new(void);
void vecu64_free(struct vecu64 **v);

#endif  /* _VECU64_H_ */
