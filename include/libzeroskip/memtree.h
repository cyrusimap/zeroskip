/*
 * memtree.h
 *
 * Memtree implementation for zeroskip - holds the in-memory data.
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#ifndef _MEMTREE_H_
#define _MEMTREE_H_

#include <stdio.h>
#include <stdint.h>

#include <libzeroskip/macros.h>

CPP_GUARD_START

#define MEMTREE_MAX_ELEMENTS 10
#define MEMTREE_MIN_ELEMENTS (MEMTREE_MAX_ELEMENTS >> 1)

enum NodeType {
        LEAF_NODE,
        INTERNAL_NODE,
};

/* Return codes */
enum {
        MEMTREE_OK             =  0,
        MEMTREE_ERROR          = -1,
        MEMTREE_INVALID        = -2,
        MEMTREE_DUPLICATE      = -3,
        MEMTREE_NOT_FOUND      = -4,
};

struct record {
        unsigned char *key;
        size_t keylen;
        unsigned char *val;
        size_t vallen;
        int deleted;
};

struct memtree_node {
        struct memtree_node *parent;

        uint32_t count;
        uint32_t depth;

        uint32_t pos;

        struct record *recs[MEMTREE_MAX_ELEMENTS];

        struct memtree_node *branches[];
};

struct memtree_iter {
        struct memtree *tree;
        struct memtree_node *node;

        uint32_t pos;

        struct record *record;
};

typedef struct memtree_iter memtree_iter_t[1];

/** Callbacks **/
/* memtree_action_cb_t should return 1 for success, for the loop to continue */
typedef int (*memtree_action_cb_t)(struct record *record, void *data);
typedef unsigned int (*memtree_search_cb_t)(const unsigned char *key,
                                          size_t keylen,
                                          struct record **recs,
                                          unsigned int count,
                                          int *found);

struct memtree {
        struct memtree_node *root;
        size_t count;

        memtree_action_cb_t destroy;
        void *destroy_data;

        memtree_search_cb_t search;
};

/* memtree_new():
 * Creates a new memtree. Takes two arguments for callbacks.
 * They can be NULL, in which case, it defaults to using the default delete
 * and search functions, which operate on `unsigned char`.
 */
struct memtree *memtree_new(memtree_action_cb_t destroy, memtree_search_cb_t search);

void memtree_free(struct memtree *tree);

/* memtree_insert_opt():
 * if `replace` is 1, replaces the value, otherwise, inserts another entry
 * with the same key/value.
 * Returns:
 *   On Success - returns MEMTREE_OK
 *   On Failure - returns non 0
 */
int memtree_insert_opt(struct memtree *tree, struct record *record, int replace);

/* memtree_insert():
 * insert a record into the tree, duplicates allowed.
 */
static inline int memtree_insert(struct memtree *tree, struct record *record)
{
        return memtree_insert_opt(tree, record, 0);
}

/* memtree_replace():
 * insert a record into the tree, replace the existing record, if the record
 * exists.
 */
static inline int memtree_replace(struct memtree *tree, struct record *record)
{
        return memtree_insert_opt(tree, record, 1);
}


/* memtree_insert_at():
 * Insert a record before the one pointed to by iter
 */
void memtree_insert_at(memtree_iter_t iter, struct record *record);

/* memtree_remove():
 * Returns:
 *   On Success - returns MEMTREE_OK
 *   On Failure - returns non 0
 */
int memtree_remove(struct memtree *tree, unsigned char *key, size_t keylen);

/* memtree_remove_at():
 * Removes the record pointed to by the iter. This function invalidates the
 * iter.
 */
int memtree_remove_at(memtree_iter_t iter);

/* memtree_deref():
 */
int memtree_deref(memtree_iter_t iter);

/* Lookup/Find functions */
int memtree_lookup(struct memtree *tree, const void *key);

/* memtree_find():
 * Return value:
 *  On Success: returns 1 with iter->element containing the match
 *  On Failure: returns 0
 */
int memtree_find(struct memtree *tree, const unsigned char *key, size_t keylen,
               memtree_iter_t iter);

int memtree_walk_forward(struct memtree *memtree, memtree_action_cb_t action,
                       void *data);
int memtree_begin(struct memtree *memtree, memtree_iter_t iter);
int memtree_prev(memtree_iter_t iter);
int memtree_next(memtree_iter_t iter);

/* These are the default callbacks that are used in the absence of
 * callbacks from the user.
 */

/* The B-Tree requires a binary search function for comparison.
 */

/* memtree_memcmp_natural():
   Sorts on natural order
 */
unsigned int memtree_memcmp_natural(const unsigned char *key, size_t keylen,
                                  struct record **recs,
                                  unsigned int count, int *found);
/* memtree_memcmp_raw():
   Sorts raw - data with common prefixes are grouped together.
 */
unsigned int memtree_memcmp_raw(const unsigned char *key, size_t keylen,
                              struct record **recs,
                              unsigned int count, int *found);

#define memtree_memcmp_fn(_name, _minlen, _cmpfn)                       \
        unsigned int memtree_memcmp_##_name(const unsigned char *key,   \
                                            size_t keylen,              \
                                            struct record **recs,       \
                                            unsigned int count,         \
                                            int *found)                 \
        {                                                               \
                unsigned int start = 0;                                 \
                const unsigned char *k = key;                           \
                while (count) {                                         \
                        unsigned int middle = count >> 1;               \
                        unsigned int pos = start + middle;              \
                        unsigned char *b;                               \
                        size_t blen = 0;                                \
                        int c = -1;                                     \
                                                                        \
                        b = (unsigned char *)recs[pos]->key;            \
                        blen = recs[pos]->keylen;                       \
                        _minlen;                                        \
                        {                                               \
                                c = _cmpfn;                             \
                                if (c == 0) {                           \
                                        if (keylen > blen)              \
                                                goto greaterthan;       \
                                        else if (keylen < blen) \
                                                goto lessthan;          \
                                        else                            \
                                                goto equals;            \
                                }                                       \
                                if (c < 0) goto lessthan;               \
                                if (c > 0) goto greaterthan;            \
                        }                                               \
                greaterthan:                                            \
                        start += middle + 1;                            \
                        count -= middle + 1;                            \
                        continue;                                       \
                equals:                                                 \
                        *found = 1;                                     \
                lessthan:                                               \
                        count = middle;                                 \
                        continue;                                       \
                }                                                       \
                 return start;                                          \
        }

int memtree_destroy(struct record *record, void *data);

int memtree_print_node_data(struct memtree *memtree, void *data);

/* Record handlers */
struct record * record_new(const unsigned char *key, size_t keylen,
                           const unsigned char *val, size_t vallen,
                           int deleted);
void record_free(struct record *record);

CPP_GUARD_END

#endif  /* _MEMTREE_H_ */
