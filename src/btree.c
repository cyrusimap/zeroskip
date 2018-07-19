/*
 * btree.c
 *
 * Btree implementation for zeroskip - holds the in-memory data.
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */
#include <libzeroskip/btree.h>
#include <libzeroskip/util.h>

#include <assert.h>
#include <string.h>

/* Default callbacks */
#define btree_default_destroy btree_destroy
#define btree_default_search  btree_memcmp_raw

/**
 * Private functions
 */

static int nodecount = 0;

static void print_rec_entry(unsigned char *d, size_t l)
{
        size_t i;
        for (i = 0; i < l; i++)
                printf("%c", d[i]);
        printf("\n");

}

/* btree_ascend()
 *  returns 0 - if iter->node does not have a parent
 *  returns 1 - if iter->node has a parent and ascends
                 iter->node->branches[iter->pos] to where iter->node
                 currently is
 */
static inline int btree_ascend(btree_iter_t iter)
{
        int ret = 0;

        if (iter->node->parent) {
                iter->pos = iter->node->pos;
                iter->node = iter->node->parent;
                ret = 1;
        }

        return ret;
}

static struct btree_node *btree_node_alloc(enum NodeType type)
{
        struct btree_node *node = NULL;
        size_t nsize;

        nsize = (type == INTERNAL_NODE) ?
                sizeof(struct btree_node) * (BTREE_MAX_ELEMENTS + 1) :
                0;

        node = xmalloc(sizeof(struct btree_node) + nsize);

        return node;
}

static void btree_node_free(struct btree_node *node, struct btree *btree)
{
        unsigned int i, count = node->count;

        if (!node->depth) {
                for (i = 0; i < count; i++) {
                        btree->destroy((void *)node->recs[i],
                                       btree->destroy_data);
                }
        } else {
                for (i = 0; i < count; i++) {
                        btree_node_free(node->branches[i], btree);
                        btree->destroy((void *)node->recs[i],
                                       btree->destroy_data);
                }
                btree_node_free(node->branches[count], btree);
        }

        xfree(node);
}

/* branch_begin()
 * given an iter, set it to the begining of the branch
 */
static void branch_begin(btree_iter_t iter)
{
        struct btree_node *node = iter->node->branches[iter->pos];
        uint32_t depth = node->depth;

        while(depth--)
                node = node->branches[0];

        iter->node = node;
        iter->pos = 0;
}


/* branch_end()
 * given an iter, set it to the end of the branch
 */
static void branch_end(btree_iter_t iter)
{
        struct btree_node *node = iter->node->branches[iter->pos];
        uint32_t depth = node->depth;

        while(depth--)
                node = node->branches[node->count];

        iter->node = node;
        iter->pos = node->count;
}

/* node_insert()
 * Inserts `key` and `val` to the right of branch `branch`
 * and into `node` at `pos`.
 */
static void node_insert(struct btree_node *branch,
                        struct btree_node *node,
                        struct record *rec,
                        uint32_t pos)
{
        uint32_t i;

        for (i = node->count; i-- > pos;) {
                node->recs[i+1] = node->recs[i];
        }

        node->recs[pos] = rec;

        if (node->depth) {
                pos++;

                for (i = node->count + 1; i-- > pos;) {
                        node->branches[i+1] = node->branches[i];
                        node->branches[i+1]->pos = i + 1;
                }

                node->branches[pos] = branch;
                branch->parent = node;
                branch->pos = pos;
        }

        node->count++;
}

/* node_split()
 * Inserts `rec` and `branch` into `node` at `pos` splitting
 * it into nodes `node`, `branch` with median element being `key`.
 */
static void node_split(struct btree_node **branch, struct btree_node *node,
                       struct record **rec, uint32_t pos)
{
        uint32_t i, split;
        struct btree_node *left = node;
        struct btree_node *right = NULL;

        if (pos <= BTREE_MIN_ELEMENTS) {
                /* if pos is <= BTREE_MIN_ELEMENTS insert into left tree,
                 * so give the left tree fewer elelemts to start with */
                split = BTREE_MIN_ELEMENTS;
        } else {
                /* if pos > BTREE_MIN_ELEMENTS insert into right subtree,
                 * so give the right tree fewer elements to start with */
                split = BTREE_MIN_ELEMENTS + 1;
        }

        if (left->depth)
                right = btree_node_alloc(INTERNAL_NODE);
        else
                right = btree_node_alloc(LEAF_NODE);

        /* The left and right subtrees are siblings, so they will have the
           same parent and depth */
        right->parent = left->parent;
        right->depth = left->depth;

        /* Initialise right side */
        for (i = split; i < BTREE_MAX_ELEMENTS; i++) {
                right->recs[i-split] = left->recs[i];
        }

        if (right->depth) {
                for (i = split+1; i <= BTREE_MAX_ELEMENTS; i++) {
                        right->branches[i-split] = left->branches[i];
                        right->branches[i-split]->parent = right;
                        right->branches[i-split]->pos = i - split;
                }
        }

        left->count = split;
        right->count = BTREE_MAX_ELEMENTS - split;

        /* Insert key/val */
        if (pos <= BTREE_MIN_ELEMENTS) {
                /* Insert into left half */
                node_insert(*branch, left, *rec, pos);
        } else {
                /* Insert into right half */
                node_insert(*branch, right, *rec, pos - split);
        }

        /* left's rightmost element is going to be the median, so give it to
           the right */
        if (right->depth) {
                right->branches[0] = left->branches[left->count];
                right->branches[0]->parent = right;
                right->branches[0]->pos = 0;
        }

        --left->count;
        *rec = (void *)left->recs[left->count];
        *branch = right;
}

static void node_move_left(struct btree_node *node, uint32_t pos)
{
        struct btree_node *left = node->branches[pos];
        struct btree_node *right = node->branches[pos + 1];
        struct btree_node *tmp = NULL;
        uint32_t i;

        left->recs[left->count] = node->recs[pos];
        node->recs[pos] = right->recs[0];

        for (i = 1; i < right->count; i++)
                right->recs[i - 1] = right->recs[i];

        if (right->depth) {
                tmp = right->branches[0];
                left->branches[left->count + 1] = tmp;
                tmp->parent = left;
                tmp->pos = left->count + 1;

                for (i = 1; i <= right->count; i++) {
                        right->branches[i - 1] = right->branches[i];
                        right->branches[i - 1]->pos = i - 1;
                }
        }

        left->count++;
        right->count--;
}

static void node_move_right(struct btree_node *node, uint32_t pos)
{
        struct btree_node *left = node->branches[pos];
        struct btree_node *right = node->branches[pos + 1];
        uint32_t i;

        for (i = right->count; i--; ) {
                right->recs[i + 1] = right->recs[i];
        }

        right->recs[0] = node->recs[pos];
        node->recs[pos] = left->recs[left->count - 1];

        if (right->depth) {
                for (i = right->count + 1; i--;) {
                        right->branches[i + 1] = right->branches[i];
                        right->branches[i + 1]->pos = i + 1;
                }

                right->branches[0] = left->branches[left->count];
                right->branches[0]->parent = right;
                right->branches[0]->pos = 0;
        }

        left->count--;
        right->count++;
}

static void node_combine(struct btree_node *node, uint32_t pos)
{
        struct btree_node *left = node->branches[pos];
        struct btree_node *right = node->branches[pos + 1];
        struct btree_node *tmp = NULL;
        struct record **rec = &left->recs[left->count];
        uint32_t i;

        *rec++ = node->recs[pos];

        for (i = 0; i < right->count; i++) {
                *rec++ = node->recs[i];
        }

        if (right->depth) {
                for (i = 0; i <= right->count; i++) {
                        tmp = right->branches[i];
                        left->branches[left->count + i + 1] = tmp;
                        tmp->parent = left;
                        tmp->pos = left->count + i + 1;
                }
        }

        for (i = pos + 1; i < node->count; i++) {
                node->recs[i - 1] = node->recs[i];

                node->branches[i] = node->branches[i + 1];
                node->branches[i]->pos = i;
        }

        left->count += right->count + 1;
        node->count--;

        xfree(right);
}

/* node_restore():
 */
static void node_restore(struct btree_node *node, uint32_t pos)
{
        if (pos == 0) {
                if (node->branches[1]->count > BTREE_MIN_ELEMENTS)
                        node_move_left(node, 0);
                else
                        node_combine(node, 0);
        } else if (pos == node->count) {
                if (node->branches[pos-1]->count > BTREE_MIN_ELEMENTS)
                        node_move_right(node, pos - 1);
                else
                        node_combine(node, pos - 1);
        } else if (node->branches[pos-1]->count > BTREE_MIN_ELEMENTS) {
                node_move_right(node, pos - 1);
        } else if (node->branches[pos+1]->count > BTREE_MIN_ELEMENTS) {
                node_move_left(node, pos);
        } else {
                node_combine(node, pos - 1);
        }
}

/* node_remove_leaf_element():
 */
static void node_remove_leaf_element(struct btree_node *node, uint32_t pos)
{
        uint32_t i;

        record_free(node->recs[pos]);

        for (i = pos + 1; i < node->count; i++) {
                node->recs[i-1] = node->recs[i];
        }

        node->count--;
}

static int node_walk_forward(const struct btree_node *node,
                             btree_action_cb_t action, void *data)
{
        uint32_t i, count;

        count = node->count;

        if (!node->depth) {
                for (i = 0; i < count; i++) {
                        action((void *)node->recs[i], data);
                }
        } else {
                for (i = 0; i < count; i++) {
                        if (!node_walk_forward(node->branches[i],
                                               action, data))
                                return 0;
                        action((void *)node->recs[i], data);
                }

                if (!node_walk_forward(node->branches[count], action, data))
                        return 0;
        }

        return 1;
}

static int node_print_data(struct record *record, void *data _unused_)
{
        size_t i;

        if (!record)
                return 0;

        for (i = 0; i < record->keylen; i++)
                printf("%c", record->key[i]);

        printf(" : ");

        for (i = 0; i < record->vallen; i++)
                printf("%c", record->val[i]);

        printf("\n");

        return 1;
}

/**
 * Public functions
 */
struct btree *btree_new(btree_action_cb_t destroy, btree_search_cb_t search)
{
        struct btree *btree = NULL;
        struct btree_node *node;

        btree = xcalloc(1, sizeof(struct btree));

        /* Root node */
        node = btree_node_alloc(LEAF_NODE);
        node->parent = NULL;
        node->count = 0;
        node->depth = 0;

        btree->root = node;

        btree->destroy = destroy ? destroy : btree_default_destroy;
        btree->search = search ? search : btree_default_search;

        return btree;
}

void btree_free(struct btree *btree)
{
        btree_node_free(btree->root, btree);
        xfree(btree);
}

int btree_begin(struct btree *btree, btree_iter_t iter)
{
        struct btree_node *node;

        iter->tree = btree;
        iter->node = btree->root;
        iter->pos = 0;
        if (iter->node->depth)
                branch_begin(iter);

        node = iter->node;
        if (node->count) {
                iter->record = node->recs[iter->pos];
                return 1;
        }

        return 0;
}

int btree_prev(btree_iter_t iter)
{
        if (iter->node->depth) {
                branch_end(iter);
        } else if (iter->pos == 0) {
                struct btree_iter t = *iter;
                do {
                        if (!btree_ascend(iter)) {
                                *iter = t;
                                return 0;
                        }
                } while (iter->pos == 0);
        }

        iter->record = (struct record *)iter->node->recs[--iter->pos];

        return 1;
}

int btree_next(btree_iter_t iter)
{
        int ret = btree_deref(iter);
        if (ret) {
                iter->pos++;
                if (iter->node->depth)
                        branch_begin(iter);
        }

        return ret;
}

int btree_insert_opt(struct btree *btree, struct record *record, int replace)
{
        btree_iter_t iter;
        int ret = BTREE_OK;

        memset(iter, 0, sizeof(iter));

        if (btree_find(btree, record->key, record->keylen, iter)) {
                if (replace) {
                        struct record *rec = iter->record;
                        xfree(rec->val);
                        rec->val = xmalloc(record->vallen + 1);
                        memcpy(rec->val, record->val, record->vallen);
                        rec->vallen = record->vallen;
                        rec->deleted = record->deleted;
                        record_free(record);
                        goto done;
                }

                ret = BTREE_DUPLICATE;
                goto done;
        }

        btree_insert_at(iter, record);

done:
        return ret;
}

int btree_remove(struct btree *btree, unsigned char *key, size_t keylen)
{
        btree_iter_t iter;

        memset(iter, 0, sizeof(iter));

        if (btree_find(btree, key, keylen, iter)) {
                btree_remove_at(iter);
                return BTREE_OK;
        }

        return BTREE_NOT_FOUND;
}

int btree_lookup(struct btree *btree _unused_,
                 const void *key _unused_)
{
        return 0;
}

unsigned int btree_memcmp_natural(const unsigned char *key, size_t keylen,
                                  struct record **recs,
                                  unsigned int count, int *found)
{
        unsigned int start = 0;
        const unsigned char *k = key;

        while (count) {
                unsigned int middle = count >> 1;
                unsigned int pos = start + middle;
                unsigned char* b;
                int c = -1;

                b = (unsigned char*)recs[pos]->key;

                if (keylen < recs[pos]->keylen)
                        goto lessthan;
                if (keylen > recs[pos]->keylen)
                        goto greaterthan;

                c = memcmp(k, b, keylen);
                if (c == 0)
                        goto equals;
                if (c < 0)
                        goto lessthan;
                if (c > 0)
                        goto greaterthan;

        greaterthan:
                start += middle + 1;
                count -= middle + 1;
                continue;
        equals:
                *found = 1;
        lessthan:
                count = middle;
                continue;
        }

        return start;
}

unsigned int btree_memcmp_raw(const unsigned char *key, size_t keylen,
                              struct record **recs,
                              unsigned int count, int *found)
{
        unsigned int start = 0;
        const unsigned char *k = key;

        while (count) {
                unsigned int middle = count >> 1;
                unsigned int pos = start + middle;
                unsigned char* b;
                int c = -1;
                size_t min;

                b = (unsigned char*)recs[pos]->key;

                min = keylen < recs[pos]->keylen ? keylen : recs[pos]->keylen;

                c = memcmp(k, b, min);
                if (c == 0) {
                        if (keylen > recs[pos]->keylen)
                                goto greaterthan;
                        else if (keylen < recs[pos]->keylen)
                                goto lessthan;
                        else
                                goto equals;
                }
                if (c < 0)
                        goto lessthan;
                if (c > 0)
                        goto greaterthan;

        greaterthan:
                start += middle + 1;
                count -= middle + 1;
                continue;
        equals:
                *found = 1;
        lessthan:
                count = middle;
                continue;
        }

        return start;
}

unsigned int btree_memcmp_mbox(const unsigned char *key, size_t keylen,
                               struct record **recs,
                               unsigned int count, int *found)
{
        unsigned int start = 0;
        const unsigned char *k = key;

        while (count) {
                unsigned int middle = count >> 1;
                unsigned int pos = start + middle;
                unsigned char* b;
                int c = -1;
                size_t min;

                b = (unsigned char*)recs[pos]->key;

                min = keylen < recs[pos]->keylen ? keylen : recs[pos]->keylen;

                c = memcmp(k, b, min);
                if (c < 0) {
                        if (keylen > recs[pos]->keylen)
                                c = 1;
                        else if(keylen < recs[pos]->keylen)
                                c = -1;
                        else
                                c = 0;
                }

                if (c == 0) {
                        if (keylen > recs[pos]->keylen)
                                goto greaterthan;
                        else if (keylen < recs[pos]->keylen)
                                goto lessthan;
                        else
                                goto equals;
                }
                if (c < 0) {
                        if (keylen > recs[pos]->keylen)
                                goto greaterthan;
                        else if (keylen < recs[pos]->keylen)
                                goto lessthan;
                        else
                                goto equals;
                }
                if (c > 0)
                        goto greaterthan;

        greaterthan:
                start += middle + 1;
                count -= middle + 1;
                continue;
        equals:
                *found = 1;
        lessthan:
                count = middle;
                continue;
        }

        return start;
}

int btree_destroy(struct record *record,
                  void *data _unused_)
{
        record_free(record);
        return 0;
}

int btree_find(struct btree *btree, const unsigned char *key, size_t keylen,
               btree_iter_t iter)
{
        struct btree_node *node = btree->root;
        uint32_t depth;
        uint32_t pos;
        int found = 0;

        iter->tree = (struct btree *)btree;
        iter->record = NULL;

        depth = node->depth;

        while (1) {
                int f = 0;
                pos = btree->search(key, keylen, node->recs, node->count, &f);
                if (f) {
                        iter->record = node->recs[pos];
                        found = 1;
                }

                if (!depth--)
                        break;

                node = node->branches[pos];
        }

        iter->node = node;
        iter->pos = pos;
        if (!found)
                iter->record = iter->node->recs[iter->pos];

        return found;
}

void btree_insert_at(btree_iter_t iter, struct record *record)
{
        struct btree_node *branch = NULL;
        struct btree_node *node = NULL;
        struct btree *btree = iter->tree;
        struct record *rec = record;

        /* Set the key/val for iter */
        iter->record = record;

        /* If the node is not a leaf, iter through to the end of the left
           branch */
        if (iter->node->depth)
                branch_end(iter);

        if (iter->node->count < BTREE_MAX_ELEMENTS) {
                /* Insert at the current node the iter points to */
                node_insert(branch, iter->node, rec, iter->pos);
                goto done;
        } else {
                /* Split the node, and try inserting the median and right
                   subtree into the parent*/
                for (;;) {
                        node_split(&branch, iter->node, &rec, iter->pos);

                        if (!btree_ascend(iter))
                                break;

                        if (iter->node->count < BTREE_MAX_ELEMENTS) {
                                node_insert(branch, iter->node, rec, iter->pos);
                                goto done;
                        }
                } /* for(;;) */

                /* If we split all the way to the root, we create a new root */
                assert(iter->node == btree->root);
                node = btree_node_alloc(INTERNAL_NODE);
                node->parent = NULL;
                node->count = 1;
                node->depth = btree->root->depth + 1;

                node->recs[0] = rec;

                node->branches[0] = btree->root;
                btree->root->parent = node;
                btree->root->pos = 0;

                node->branches[1] = branch;
                branch->parent = node;
                branch->pos = 1;

                btree->root = node; /* The new root */
        }         /* else */

done:
        btree->count++;
        iter->node = NULL;
}

int btree_deref(btree_iter_t iter)
{

        if (iter->pos >= iter->node->count) {
                struct btree_iter tmp = *iter;
                do {
                        if (!btree_ascend(iter)) {
                                *iter = tmp;
                                return 0;
                        }
                } while (iter->pos >= iter->node->count);
        }

        iter->record = iter->node->recs[iter->pos];

        return 1;
}

int btree_remove_at(btree_iter_t iter)
{
        struct btree *btree = iter->tree;
        struct btree_node *root = NULL;

        if (!btree_deref(iter))
                return 0;

        /* record_free(iter->record); */

        if (!iter->node->depth) {
                node_remove_leaf_element(iter->node, iter->pos);
                if (iter->node->count >= BTREE_MIN_ELEMENTS ||
                    !iter->node->parent)
                        goto done;
        } else {
                /* Save pointers to the data that needs to be removed*/
                struct record **rec = &iter->node->recs[iter->pos];

                /* Start branching */
                iter->pos++;
                branch_begin(iter);

                /* Replace with the successor */
                *rec = iter->node->recs[0];
                print_rec_entry(iter->node->recs[iter->pos]->key,
                                iter->node->recs[iter->pos]->keylen);

                node_remove_leaf_element(iter->node, 0);
        }

        while (1) {
                if (iter->node->count >= BTREE_MIN_ELEMENTS)
                        goto done;

                if (!btree_ascend(iter))
                        break;

                node_restore(iter->node, iter->pos);
        }

        /* We've got to the root after combining */
        root = iter->node;
        assert(root == btree->root);
        assert(root->depth > 0);
        if (root->count == 0) {
                btree->root = root->branches[0];
                btree->root->parent = NULL;
                xfree(root);
        }

done:
        btree->count--;
        iter->node = NULL;
        return 1;
}

int btree_walk_forward(struct btree *btree, btree_action_cb_t action, void *data)
{
        return node_walk_forward(btree->root, action, data);
}

int btree_print_node_data(struct btree *btree, void *data)
{
        return btree_walk_forward(btree, node_print_data, data);
}

struct record * record_new(const unsigned char *key, size_t keylen,
                           const unsigned char *val, size_t vallen,
                           int deleted)
{
        struct record *rec = NULL;

        rec = xmalloc(sizeof(struct record));

        rec->key = xmalloc(keylen + 1);
        memcpy(rec->key, key, keylen);
        rec->keylen = keylen;

        rec->val = xmalloc(vallen + 1);
        memcpy(rec->val, val, vallen);
        rec->vallen = vallen;

        rec->deleted = deleted;

        nodecount++;
        return rec;
}

void record_free(struct record *record)
{
        assert(record);

        xfree(record->key);
        xfree(record->val);
        record->keylen = 0;
        record->vallen = 0;
        record->deleted = 0;

        xfree(record);

        nodecount--;
}
