/*
 * bptree.h
 *
 * B+tree implementation for zeroskip - holds the in-memory data.
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */
#ifndef _BPTREE_H_
#define _BPTREE_H_

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>

#define BTREE_MAX_ELEMENTS 10
#define BTREE_MIN_ELEMENTS (BTREE_MAX_ELEMENTS >> 1)

/* Return codes */
enum {
        BTREE_OK             =  0,
        BTREE_ERROR          = -1,
        BTREE_INVALID        = -2,
        BTREE_DUPLICATE      = -3,
        BTREE_NOT_FOUND      = -4,
};

struct record {
        unsigned char *key;
        size_t keylen;
        unsigned char *val;
        size_t vallen;
};

struct bptree_node {
        void **elements;
        unsigned char **keys;
        struct bptree_node *parent;

        bool is_leaf;
        uint32_t numkeys;
        struct bptree_node *next;
};

struct bptree {
        struct bptree_node *root;
        size_t count;
};

struct bptree *bptree_new(void);
void bptree_free(struct bptree **tree);

/* bptree_insert():
 * Returns:
 *   On Success - returns BTREE_OK
 *   On Failure - returns non 0
 */
int bptree_insert(struct bptree *tree, struct record *record);


/* Record handlers */
struct record * record_new(unsigned char *key, size_t keylen,
                           unsigned char *val, size_t vallen);
void record_free(struct record *record);


#endif  /* _BPTREE_H_ */
