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

#include <stdint.h>

#include "macros.h"

CPP_GUARD_START
typedef struct _hashTable HashTable;
typedef struct _hashEntry HashEntry;
typedef struct _hashProps HashProps;

typedef enum {
        HT_SUCCESS = 0,
        HT_INSERTED = -1,
        HT_ERROR = -2,
        HT_EXISTS = -3,
        HT_INDEX_ERR = -4,
        HT_EMPTY = -5
} HtStatus;

struct _hashProps {
        unsigned int (*hash_fn)(const void *key);
        int (*key_cmp_fn)(const void *key1, const void *key2);

        void *(*key_dup_fn)(const void *key);
        void *(*val_dup_fn)(const void *val);

        void (*key_free_fn)(void *key);
        void (*val_free_fn)(void *val);
};

#define HT_CMP_KEYS(h, key1, key2)               \
        (((h)->props->key_cmp_fn) ?              \
         (h)->props->key_cmp_fn(key1, key2) :    \
         (key1) == (key2))

#define HT_DUP_KEY(h, entry, k) do {                             \
                if ((h)->props->key_dup_fn)                      \
                        entry->key = (h)->props->key_dup_fn(k);  \
                else                                             \
                        entry->key = k;                          \
        } while(0)

#define HT_FREE_KEY(h, entry)                           \
        if ((h)->props->key_free_fn)                    \
                (h)->props->key_free_fn((entry)->key)

#define HT_DUP_VAL(h, entry, v) do {                             \
                if ((h)->props->val_dup_fn)                      \
                        entry->val = (h)->props->val_dup_fn(v);  \
                else                                             \
                        entry->val = v;                          \
        } while(0)

#define HT_FREE_VAL(h, entry)                           \
        if ((h)->props->val_free_fn)                    \
                (h)->props->val_free_fn((entry)->val)


HashTable *ht_new(HashProps *props);
void ht_free(HashTable *ht);

HtStatus ht_insert(HashTable *ht, void *key, void *val);
HtStatus ht_delete(HashTable *ht, const void *key);
void *ht_find(HashTable *ht, void *key);
HtStatus ht_replace(HashTable *ht, void *key, void *newval);

void ht_set_hash_function_seed(uint32_t seed);
uint32_t ht_get_hash_function_seed(void);
unsigned int murmur3_hash_32(const void *key, int len);

unsigned long ht_get_capacity(HashTable *ht);
unsigned long ht_get_size(HashTable *ht);

CPP_GUARD_END
#endif  /* _HTABLE_H_ */
