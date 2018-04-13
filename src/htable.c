/*
 * htable.h : A generic hash table implementation
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */
#include "htable.h"
#include "util.h"

#include <stdio.h>
#include <string.h>
#include <limits.h>

struct _hashEntry {
        void *key;
        void *val;
        struct _hashEntry *next;
};

struct _ht {
        unsigned long size;     /* unsigned long: 0 to 4,294,967,295 */
        unsigned long used;
        unsigned long sizemask;
        struct _hashEntry **entries;
};

struct _hashTable {
        long rehash_index;
        unsigned long iters;
        HashProps *props;
        struct _ht table[2];
};


#define ROTL32(x, y) ((x << y) | (x >> (32 - y)))

/* Globals */
static uint32_t hash_func_seed = 5381;

/*
 * Private Methods
 */

/*
 * The hash table's size is based on the power of two. So given a size,
 * this function computes the nearest(ceiling), size that will be allocated
 * for a hash table.
 */
static unsigned long compute_actual_size(unsigned long size)
{
        unsigned long i = 2;

        if (size >= LONG_MAX)
                return LONG_MAX;

        while (1) {
                if (i >= size)
                        return i;
                i *= 2;
        }
}

static void ht_reset(struct _ht *t)
{
        t->size = 0;
        t->used = 0;
        t->sizemask = 0;
        t->entries = NULL;
}

static void _ht_free(HashTable *ht, struct _ht *table)
{
        unsigned long i;

        for (i = 0; i < table->size && table->used > 0; i++) {
                HashEntry *e, *next;

                e = table->entries[i];
                if (e == NULL)
                        continue;

                while (e) {
                        next = e->next;

                        HT_FREE_KEY(ht, e);
                        HT_FREE_VAL(ht, e);
                        xfree(e);

                        table->used--;
                        e = next;
                } /* while() */
        } /* for() */

        xfree(table->entries);
        ht_reset(table);
}

/* Incremental rehash in 'steps'. */
static HtStatus ht_rehash(HashTable *ht, int steps)
{
        if (ht->rehash_index != -1)
                return HT_SUCCESS;

        while (steps-- && ht->table[0].used != 0) {
                HashEntry *e, *next;

                /* Do nothing with empty entries */
                while (ht->table[0].entries[ht->rehash_index] == NULL)
                        ht->rehash_index++;

                e = ht->table[0].entries[ht->rehash_index];

                /* Move keys */
                while (e) {
                        unsigned int hash;

                        next = e->next;

                        /* Get index in the new hash table */
                        hash = ht->props->hash_fn(e->key) & ht->table[1].sizemask;
                        e->next = ht->table[1].entries[hash];
                        ht->table[1].entries[hash] = e;
                        ht->table[0].used--;
                        ht->table[1].used++;
                        e = next;
                } /* while() */

                ht->table[0].entries[ht->rehash_index] = NULL;
                ht->rehash_index++;
        } /* while() */

        /* Are we done rehashing? */
        if (ht->table[0].used == 0) {
                xfree(ht->table[0].entries);
                ht->table[0] = ht->table[1];
                ht_reset(&ht->table[1]);
                ht->rehash_index = -1;
                return HT_SUCCESS;
        }

        return HT_ERROR;
}

static void ht_rehash_if_possible(HashTable *ht)
{
        if (ht->iters == 0)
                ht_rehash(ht, 1);
}

static HtStatus ht_resize(HashTable *ht, unsigned long size)
{
        struct _ht t;
        unsigned long newsize;

        if (ht->rehash_index != -1 || ht->table[0].used > size)
                return HT_ERROR;

        newsize = compute_actual_size(size);
        if (newsize == ht->table[0].size)
                return HT_ERROR;

        t.size = newsize;
        t.sizemask = newsize - 1;
        t.entries = xcalloc(newsize, sizeof(HashEntry));
        t.used = 0;

        /* A new table */
        if (ht->table[0].entries == NULL) {
                ht->table[0] = t;
                return HT_SUCCESS;
        }

        /* Resizing an existing table */
        ht->table[1] = t;
        ht->rehash_index = 0;

        return HT_SUCCESS;
}

static HtStatus ht_resize_if_needed(HashTable *ht)
{

        if (ht->rehash_index != -1) /* Rehashing, just return! */
                return HT_SUCCESS;

        if (ht->table[0].size == 0) /* A new hashtable. */
                ht_resize(ht, 4);

        if (ht->table[0].used >= ht->table[0].size) {
                return ht_resize(ht, ht->table[0].size*2);
        }

        return HT_SUCCESS;
}

/*
 * Function returns the index of key, if successfully found,
 * a (-1) otherwise.
 */
static int ht_get_index(HashTable *ht, void *key)
{
        unsigned int hash, index = -1, table;
        HashEntry *e;

        if (ht_resize_if_needed(ht) != 0) {
                return -1;
        }

        hash = ht->props->hash_fn(key);

        for (table = 0; table <= 1; table++) {
                index = hash & ht->table[table].sizemask;
                e = ht->table[table].entries[index];
                while (e) {
                        if (HT_CMP_KEYS(ht, key, e->key))
                                return -1;

                        e = e->next;
                }

                /* the hash table isn't being rehashed, so there's no
                 * need to look into the second table. */
                if (ht->rehash_index == -1)
                        break;
        }

        return index;
}

static HashEntry *ht_insert_key(HashTable *ht, void *key)
{
        int index;
        HashEntry *e;
        struct _ht *t;

        if (ht_resize_if_needed(ht) != 0) {
                ht_rehash_if_possible(ht);
        }

        index = ht_get_index(ht, key);
        if (index == -1)
                return NULL;

        if (ht->rehash_index != -1)
                t = &ht->table[1];
        else
                t = &ht->table[0];

        e = xmalloc(sizeof(HashEntry));
        e->next = t->entries[index];
        t->entries[index] = e;
        t->used++;

        HT_DUP_KEY(ht, e, key);

        return e;
}

static HtStatus ht_delete_key(HashTable *ht, const void *key)
{
        unsigned int index, hash;
        int table;
        HashEntry *e, *prev;

        if (ht->table[0].size == 0)
                return HT_EMPTY;

        if (ht->rehash_index != -1)
                ht_rehash_if_possible(ht);

        hash = ht->props->hash_fn(key);

        for (table = 0; table <= 1; table++) {
                index = hash & ht->table[1].sizemask;
                e = ht->table[table].entries[index];
                prev = NULL;

                while (e) {
                        if (HT_CMP_KEYS(ht, key, e->key)) {
                                if (prev)
                                        prev->next = e->next;
                                else
                                        ht->table[table].entries[index] = e->next;

                                HT_FREE_KEY(ht, e);
                                HT_FREE_VAL(ht, e);

                                xfree(e);

                                ht->table[table].used--;

                                return HT_SUCCESS;
                        }

                        prev = e;
                        e = e->next;
                } /* while() */

                /* the hash table isn't being rehashed, so there's no
                 * need to look into the second table. */
                if (ht->rehash_index == -1)
                        break;
        } /* for() */

        return HT_ERROR;
}

static HashEntry *ht_find_entry(HashTable *ht, void *key)
{
        HashEntry *e;
        unsigned int hash, table, index;

        if (ht->table[0].size == 0)
                return NULL;

        if (ht->rehash_index != -1)
                ht_rehash_if_possible(ht);

        hash = ht->props->hash_fn(key);

        for (table = 0; table <=1; table++) {
                index = hash & ht->table[table].sizemask;
                e = ht->table[table].entries[index];

                while (e) {
                        if (HT_CMP_KEYS(ht, key, e->key)) {
                                return e;
                        }

                        e = e->next;
                }

                if (ht->rehash_index == -1)
                        return NULL;
        }

        return NULL;
}


/*
 * Public Methods
 */

/*
 * This is an implementation of the mururmur3 hash by Austin Appleby:
 * https://github.com/aappleby/smhasher
 */
unsigned int murmur3_hash_32(const void *key, int len)
{
        uint32_t hash = hash_func_seed;
        const int nblocks = len / 4;
        const uint32_t *blocks = (const uint32_t *)key;
        const uint8_t *data = (const uint8_t *)key;
        int i;
        uint32_t k = 0;
        const uint8_t *tail;

        /* constants */
        uint32_t c1 = 0xcc9e2d51;
        uint32_t c2 = 0x1b873593;
        uint32_t n = 0xe6546b64;
        /* rotations */
        uint32_t r1 = 15;
        uint32_t r2 = 13;
        /* multiplicant */
        uint32_t m = 5;


        /* Body */
        for (i = 0; i < nblocks; i++) {
                uint32_t k1 = blocks[i];
                k1 *= c1;
                k1 = ROTL32(k1, r1);
                k1 *= c2;

                hash ^= k1;
                hash = ROTL32(hash, r2);
                hash = hash*m+n;
        }

        /* Tail */
        tail = (const uint8_t *)(data + nblocks * 4);
        switch (len & 3) {
        case 3: k ^= tail[2] << 16;
            _fallthrough_;
        case 2: k ^= tail[1] << 8;
            _fallthrough_;
        case 1: k ^= tail[0];
                k *= c1;
                k = ROTL32(k, r1);
                k *= c2;
                hash ^= k;
        }

        /* Finalisation */
        hash ^= len;
        hash ^= (hash >> 16);
        hash *= 0x85ebca6b;
        hash ^= (hash >> 13);
        hash *= 0xc2b2ae35;
        hash ^= (hash >> 16);

        return hash;
}

void ht_set_hash_function_seed(uint32_t seed)
{
        hash_func_seed = seed;
}

uint32_t ht_get_hash_function_seed(void)
{
        return hash_func_seed;
}


HashTable *ht_new(HashProps *props)
{
        HashTable *ht = NULL;

        ht = xmalloc(sizeof(HashTable));

        ht_reset(&ht->table[0]);
        ht_reset(&ht->table[1]);

        ht->props = props;
        ht->rehash_index = -1;

        return ht;
}


void ht_free(HashTable *ht)
{
        _ht_free(ht, &ht->table[0]);
        _ht_free(ht, &ht->table[1]);

        ht->props = NULL;
        ht->rehash_index = -1;

        xfree(ht);
}

HtStatus ht_insert(HashTable *ht, void *key, void *val)
{
        HashEntry *e;

        e = ht_insert_key(ht, key);
        if (!e)
                return HT_ERROR;

        HT_DUP_VAL(ht, e, val);

        return HT_SUCCESS;
}

HtStatus ht_delete(HashTable *ht, const void *key)
{
        return ht_delete_key(ht, key);
}

void *ht_find(HashTable *ht, void *key)
{
        HashEntry *e;

        e = ht_find_entry(ht, key);

        return e ? e->val : NULL;
}

/*
 * This function replaces the value of a given `key` with `newval`.
 * If the key doesn't exist, it just adds it and returns a HT_ADDED.
 * Returns a HT_SUCCESS if the value is replaced successfully, HT_ERROR
 * otherwise.
 */
HtStatus ht_replace(HashTable *ht, void *key, void *newval)
{
        HashEntry *e, *tmp_e;

        if (ht_insert(ht, key, newval) == HT_SUCCESS)
                return HT_INSERTED;

        e = ht_find_entry(ht, key);

        tmp_e = e;
        HT_DUP_VAL(ht, e, newval);
        HT_FREE_VAL(ht, tmp_e);

        return HT_SUCCESS;
}

unsigned long ht_get_capacity(HashTable *ht)
{
        return (ht->table[0].size + ht->table[1].size);
}

unsigned long ht_get_size(HashTable *ht)
{
        return (ht->table[0].used + ht->table[1].used);
}
