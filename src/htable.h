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
        int (*key_cmp_fn)(const void *key1, size_t len1,
                          const void *key2, size_t len2);

        void *(*key_dup_fn)(const void *key, size_t len);
        void *(*val_dup_fn)(const void *val, size_t len);

        void (*key_free_fn)(void *key);
        void (*val_free_fn)(void *val);
};

#define HT_CMP_KEYS(h, key1, len1, key2, len2)                  \
        (((h)->props->key_cmp_fn) ?                             \
         (h)->props->key_cmp_fn(key1, len1, key2, len2) :       \
         (key1) == (key2))

#define HT_DUP_KEY(h, entry, k, len) do {                               \
                if ((h)->props->key_dup_fn) {                           \
                        entry->key = (h)->props->key_dup_fn(k, len);    \
                } else {                                                \
                        entry->key = k;                                 \
                        entry->keylen = len;                            \
                }                                                       \
        } while(0)

#define HT_FREE_KEY(h, entry)                           \
        if ((h)->props->key_free_fn)                    \
                (h)->props->key_free_fn((entry)->key)

#define HT_DUP_VAL(h, entry, v, len) do {                               \
                if ((h)->props->val_dup_fn) {                           \
                        entry->val = (h)->props->val_dup_fn(v, len);    \
                } else {                                                \
                        entry->val = v;                                 \
                        entry->vallen = len;                            \
                }                                                       \
        } while(0)

#define HT_FREE_VAL(h, entry)                           \
        if ((h)->props->val_free_fn)                    \
                (h)->props->val_free_fn((entry)->val)


HashTable *ht_new(HashProps *props);
void ht_free(HashTable *ht);

HtStatus ht_insert(HashTable *ht, void *key, size_t keylen,
                   void *val, size_t vallen);
HtStatus ht_delete(HashTable *ht, const void *key, size_t keylen);
void *ht_find(HashTable *ht, void *key, size_t keylen);
HtStatus ht_replace(HashTable *ht, void *key, size_t keylen,
                    void *newval, size_t newvallen);

void ht_set_hash_function_seed(uint32_t seed);
uint32_t ht_get_hash_function_seed(void);
unsigned int murmur3_hash_32(const void *key, size_t len);

unsigned long ht_get_capacity(HashTable *ht);
unsigned long ht_get_size(HashTable *ht);

CPP_GUARD_END
#endif  /* _HTABLE_H_ */
