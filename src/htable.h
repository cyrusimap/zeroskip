/*
 * htable.h : A generic hash table implementation
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */
#ifndef _HTABLE_H_
#define _HTABLE_H_

#include <stdio.h>
#include <stdint.h>

#include <libzeroskip/macros.h>

CPP_GUARD_START

unsigned int bufhash(const void *buf, size_t len);

/* Comparison function */
typedef int (*htable_cmp_fn)(const void *data, const void *entry1,
                             const void *entry2, const void *kdata);

/* struct htable_entry represents an entry in the hash table
 * and should be the first member in the user data structure for
 * the entry.
 */
struct htable_entry {
        struct htable_entry *next; /* next element in case of collision */
        unsigned int hash;
};

struct htable {
        struct htable_entry **table;

        htable_cmp_fn cmpfn;
        const void *cmpfndata;

        unsigned int count;      /* number of entries */
        size_t size;             /* allocated size of the table */

        unsigned int grow_mark;
        unsigned int shrink_mark;
};

extern void htable_init(struct htable *ht, htable_cmp_fn cmp_fn,
                        const void *cmpfndata, size_t size);
extern void htable_free(struct htable *ht, int free_entries);

/* htable_entry_init():
 *   initialise htable entry
 */
static inline void htable_entry_init(void *entry, unsigned int hash)
{
        struct htable_entry *e = entry;
        e->hash = hash;
        e->next = NULL;
}

/* htable_get():
 *  get the entry for a given key, NULL otherwise.
 */
extern void *htable_get(const struct htable *ht, const void *key,
                        const void *keydata);

/* htable_get_next():
 *  get the 'next' entry in the hash table, when there are duplicate entries
 * becuase of collision, NULL if there are no duplicate entries. `entry` is
 * where the lookup should start from.
 */
extern const void *htable_get_next(const struct htable *ht, const void *entry);

/* htable_put():
 *  adds a entry into the hash table. Allows duplicate entries.
 */
extern void htable_put(struct htable *ht, void *entry);

/* htable_replace():
 *  replace an entry in the hash table, if it exists. If the entry doesn't
 * exist, it just adds a new entry(works like htable_add()).
 * In case of replacement, it returns the current value of the entry.
 */
extern void *htable_replace(struct htable *ht, void *entry);

/* htable_remove():
 *  remove an entry in the hash table matching a specified key. If the key
 * contains duplicate entries, only one will be removed. Returns the removed
 * entry or NULL if no entry exists.
 */
extern void *htable_remove(struct htable *ht, const void *key,
                           const void *keydata);

/* hashtable iterator */
struct htable_iter {
        struct htable *ht;
        struct htable_entry *next;
        unsigned int pos;
};

extern void htable_iter_init(struct htable *ht, struct htable_iter *iter);

extern void *htable_iter_next(struct htable_iter *iter);

static inline void *htable_iter_first(struct htable *ht,
                                      struct htable_iter *iter)
{
        htable_iter_init(ht, iter);
        return htable_iter_next(iter);
}

CPP_GUARD_END
#endif  /* _HTABLE_H_ */
