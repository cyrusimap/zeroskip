/*
 * bptree.c
 *
 * B+tree implementation for zeroskip - holds the in-memory data.
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#include "bptree.h"
#include "util.h"

#include <assert.h>
#include <string.h>

/*
 * Private functions
 */
static void bptree_nodes_free(struct bptree_node *node)
{
        uint32_t i;

        if (node->is_leaf) {
                for (i = 0; i < node->numkeys; i++)
                        record_free(node->elements[i]);
        } else {
                for (i = 0; i < node->numkeys; i++)
                        bptree_nodes_free(node->elements[i]);
        }

        xfree(node->elements);
        xfree(node->keys);
        xfree(node);
}

/*
 * Public functions
 */
struct bptree *bptree_new(void)
{
        struct bptree *tree = NULL;

        tree = xcalloc(1, sizeof(struct bptree));

        tree->root = NULL;
        tree->count = 0;

        return tree;
}

void bptree_free(struct bptree **tree)
{
        if (tree && *tree) {
                struct bptree *t = *tree;
                bptree_nodes_free(t->root);
                t->count = 0;
                xfree(t);
                *tree = NULL;
        }
}


struct record * record_new(unsigned char *key, size_t keylen,
                           unsigned char *val, size_t vallen)
{
        struct record *rec = NULL;

        rec = xmalloc(sizeof(struct record));

        rec->key = xmalloc(keylen + 1);
        memcpy(rec->key, key, keylen);
        rec->keylen = keylen;

        rec->val = xmalloc(vallen + 1);
        memcpy(rec->val, val, vallen);
        rec->vallen = vallen;

        return rec;
}

void record_free(struct record *record)
{
        assert(record);

        xfree(record->key);
        xfree(record->val);
        record->keylen = 0;
        record->vallen = 0;
        xfree(record);
}
