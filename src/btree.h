/*
 * zs-btree.h
 *
 * Btree implementation for zeroskip - holds the in-memory data.
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#ifndef _BTREE_H_
#define _BTREE_H_

#include <stdio.h>
#include <stdint.h>

#include "macros.h"

CPP_GUARD_START

#define BTREE_MAX_ELEMENTS 10
#define BTREE_MIN_ELEMENTS (BTREE_MAX_ELEMENTS >> 1)

enum NodeType {
        LEAF_NODE,
        INTERNAL_NODE,
};

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

struct btree_node {
        struct btree_node *parent;

        uint32_t count;
        uint32_t depth;

        uint32_t pos;

        struct record *recs[BTREE_MAX_ELEMENTS];

        struct btree_node *branches[];
};

struct btree_iter {
        struct btree *tree;
        struct btree_node *node;

        uint32_t pos;

        struct record *record;
};

typedef struct btree_iter btree_iter_t[1];

/** Callbacks **/
/* btree_action_cb_t should return 1 for success, for the loop to continue */
typedef int (*btree_action_cb_t)(struct record *record, void *data);
typedef unsigned int (*btree_search_cb_t)(unsigned char *key,
                                          size_t keylen,
                                          struct record **recs,
                                          unsigned int count,
                                          int *found);

struct btree {
        struct btree_node *root;
        size_t count;

        btree_action_cb_t destroy;
        void *destroy_data;

        btree_search_cb_t search;
};

/* btree_new():
 * Creates a new btree. Takes two arguments for callbacks.
 * They can be NULL, in which case, it defaults to using the default delete
 * and search functions, which operate on `unsigned char`.
 */
struct btree *btree_new(btree_action_cb_t destroy, btree_search_cb_t search);

void btree_free(struct btree *tree);

/* btree_insert_opt():
 * if `replace` is 1, replaces the value, otherwise, inserts another entry
 * with the same key/value.
 * Returns:
 *   On Success - returns BTREE_OK
 *   On Failure - returns non 0
 */
int btree_insert_opt(struct btree *tree, struct record *record, int replace);

/* btree_insert():
 * insert a record into the tree, duplicates allowed.
 */
static inline int btree_insert(struct btree *tree, struct record *record)
{
        return btree_insert_opt(tree, record, 0);
}

/* btree_replace():
 * insert a record into the tree, replace the existing record, if the record
 * exists.
 */
static inline int btree_replace(struct btree *tree, struct record *record)
{
        return btree_insert_opt(tree, record, 1);
}


/* btree_insert_at():
 * Insert a record before the one pointed to by iter
 */
void btree_insert_at(btree_iter_t iter, struct record *record);

/* btree_remove():
 * Returns:
 *   On Success - returns BTREE_OK
 *   On Failure - returns non 0
 */
int btree_remove(struct btree *tree, unsigned char *key, size_t keylen);

/* btree_remove_at():
 * Removes the record pointed to by the iter. This function invalidates the
 * iter.
 */
int btree_remove_at(btree_iter_t iter);

/* btree_deref():
 */
int btree_deref(btree_iter_t iter);

/* Lookup/Find functions */
int btree_lookup(struct btree *tree, const void *key);

/* btree_find():
 * Return value:
 *  On Success: returns 1 with iter->element containing the match
 *  On Failure: returns 0
 */
int btree_find(struct btree *tree, unsigned char *key, size_t keylen,
               btree_iter_t iter);

int btree_walk_forward(struct btree *btree, btree_action_cb_t action,
                       void *data);
int btree_prev(btree_iter_t iter);
int btree_next(btree_iter_t iter);

/* These are the default callbacks that are used in the absence of
 * callbacks from the user.
 */

/* The B-Tree requires a binary search function for comparison.
 */
unsigned int btree_memcmp(unsigned char *key, size_t keylen,
                          struct record **recs,
                          unsigned int count, int *found);

int btree_destroy(struct record *record, void *data);

int btree_print_node_data(struct btree *btree, void *data);

/* Record handlers */
struct record * record_new(unsigned char *key, size_t keylen,
                           unsigned char *val, size_t vallen);
void record_free(struct record *record);

CPP_GUARD_END

#endif  /* _BTREE_H_ */
