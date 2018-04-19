/*
 * zeroskip-transactions.c
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 */

#include "log.h"
#include "util.h"
#include "zeroskip.h"
#include "zeroskip-priv.h"

void assert_zsdb(struct zsdb *db);
/**
 * Private functions
 */
/* hash table handling */
static struct txn_htable_entry *alloc_txn_htable_entry(unsigned char *key,
                                                       size_t keylen,
                                                       void *value)
{
        struct txn_htable_entry *e;

        e = xmalloc(sizeof(struct txn_htable_entry));
        e->key = xmalloc(sizeof(unsigned char) * keylen);
        memcpy(e->key, key, keylen);
        e->keylen = keylen;
        e->value = value;

        return e;
}

static void free_txn_htable_entry(struct txn_htable_entry **entry)
{
        if (entry && *entry) {
                xfree((*entry)->key);
                (*entry)->keylen = 0;
                (*entry)->value = NULL;
                xfree(*entry);
        }
}

static int txn_htable_entry_cmpfn(const void *unused1 _unused_,
                                  const void *entry1,
                                  const void *entry2,
                                  const void *unused2 _unused_)
{
        const struct txn_htable_entry *e1 = entry1;
        const struct txn_htable_entry *e2 = entry2;

        return memcmp_raw(e1->key, e1->keylen, e2->key, e2->keylen);
}

static void txn_htable_init(struct txn_htable *ht)
{
        htable_init(&ht->table, txn_htable_entry_cmpfn, NULL, 0);
}

static void *txn_htable_get(struct txn_htable *ht, const unsigned char *key,
                            size_t keylen)
{
        struct txn_htable_entry k;
        struct txn_htable_entry *e;

        if (!ht->table.size)
                txn_htable_init(ht);

        htable_entry_init(&k, bufhash(key, keylen));
        k.key = (unsigned char *)key;
        k.keylen = keylen;
        e = htable_get(&ht->table, &k, NULL);

        return e ? e : NULL;
}

static void txn_htable_put(struct txn_htable *ht, unsigned char *key,
                           size_t keylen, void *value)
{
        struct txn_htable_entry *e;

        if (!ht->table.size)
                txn_htable_init(ht);

        e = alloc_txn_htable_entry(key, keylen, value);
        htable_entry_init(e, bufhash(key, keylen));

        htable_put(&ht->table, e);
}

static void *txn_htable_remove(struct txn_htable *ht, const unsigned char *key,
                               size_t keylen)
{
        struct txn_htable_entry e;

        if (!ht->table.size)
                txn_htable_init(ht);

        htable_entry_init(&e, bufhash(key, keylen));

        return htable_remove(&ht->table, &e, key);
}

static void txn_htable_free(struct txn_htable *ht)
{
        struct htable_iter iter;
        struct txn_htable_entry *e;

        htable_iter_init(&ht->table, &iter);
        while ((e = htable_iter_next(&iter))) {
                free_txn_htable_entry(&e);
        }

        htable_free(&ht->table, 0);
}

/* iter data handling */
static struct iter_key_data *alloc_iter_key_data(unsigned char *key,
                                                 size_t keylen)
{
        struct iter_key_data *data = NULL;

        data = xmalloc(sizeof(struct iter_key_data));

        data->key = xmalloc(sizeof(unsigned char) * keylen);
        memcpy(data->key, key, keylen);
        data->len = keylen;

        return data;
}

static void free_iter_key_data(struct iter_key_data **data)
{
        if (data && *data) {
                xfree((*data)->key);
                (*data)->len = 0;
                xfree(*data);
        }
}

static int iter_pq_cmp(const void *d1, const void *d2, void *cbdata _unused_)
{
        struct iter_key_data *e1, *e2;

        e1 = (struct iter_key_data *)d1;
        e2 = (struct iter_key_data *)d2;

        return memcmp_raw(e1->key, e1->len, e2->key, e2->len);
}

/* struct txn_data handling */
static struct txn_data *txn_data_alloc(zsdb_be_t type, int prio, void *data)
{
        struct txn_data *d;

        d = xcalloc(1, sizeof(struct txn_data));

        d->type = type;
        d->priority = prio;
        d->done = 0;
        if (type == ZSDB_BE_PACKED) {
                /* data is in files */
                d->data.f = data;
        } else {
                /* data is an in-memory btree for finalised and active */
                struct btree *tree = data;
                btree_begin(tree, d->data.iter);
                btree_next(d->data.iter);
        }

        return d;
}

static void txn_data_free(struct txn_data **txnd)
{
        if (txnd && *txnd) {
                struct txn_data *data = *txnd;
                *txnd = NULL;

                xfree(data);
        }
}

static void txn_datav_add_txn(struct txn *txn, struct txn_data *txnd)
{
        ALLOC_GROW(txn->datav, txn->txn_data_count + 1, txn->txn_data_alloc);
        txn->datav[txn->txn_data_count++] = txnd;
        txn->datav[txn->txn_data_count] = NULL;
}

static void txn_datav_clear_txn(struct txn *txn)
{
        if (txn->datav != NULL) {
                int i;
                for (i = 0; i < txn->txn_data_count; i++) {
                        struct txn_data *d = txn->datav[i];
                        txn_data_free(&d);
                }
                xfree(txn->datav);
        }
}

static void txn_data_process(struct txn *txn,
                             unsigned char *key, size_t keylen,
                             struct txn_data *new_txnd);
/* Get next entry in txn_data */
static int txn_data_next(struct txn *txn, struct txn_data *txndata)
{
        unsigned char *key = NULL;
        size_t keylen = 0;
        int ret = 0;

        if (!txndata)
                return ret;

        if (txndata->done)
                return 0;

        switch (txndata->type) {
        case ZSDB_BE_ACTIVE:
        case ZSDB_BE_FINALISED:
                if (btree_next(txndata->data.iter)) {
                        key = txndata->data.iter->record->key;
                        keylen = txndata->data.iter->record->keylen;
                }
                break;
        case ZSDB_BE_PACKED:
        {
                struct zsdb_file *f = txndata->data.f;
                f->indexpos++;
                if (f->indexpos < f->index->count)
                        zs_packed_file_get_key_from_offset(f, &key, &keylen);

                break;
        }
        default:
                abort();        /* should never reach here */
        }

        if (!key) {
                txndata->done = 1;
                ret = 0;
        } else {
                txn_data_process(txn, key, keylen, txndata);
                ret = 1;
        }

        return ret;
}

/* Process struct txn_data for iteration */
static void txn_data_process(struct txn *txn,
                             unsigned char *key, size_t keylen,
                             struct txn_data *new_txnd)
{
        struct txn_htable_entry *e;

        /* check if the key exists in the hash table */
        e = txn_htable_get(&txn->ht, key, keylen);
        if (e) {                /* key exists */
                /* If the key exists, we check if the priority of the existing
                 * entry is less than that of the new entry
                 */
                struct txn_data *old_txnd = e->value;
                if (new_txnd->priority > old_txnd->priority) {
                        /* Replace it with new txn_data */
                        e = txn_htable_remove(&txn->ht, key, keylen);
                        free_txn_htable_entry(&e);
                        txn_htable_put(&txn->ht, key, keylen, new_txnd);
                        /* Process the old txnd */
                        txn_data_next(txn, old_txnd);
                } else {
                        /* If the new txnd's priority is greater, we reprocess.
                         */
                        txn_data_next(txn, new_txnd);
                }
        } else {                /* new key */
                struct iter_key_data *pqd;
                pqd = alloc_iter_key_data(key, keylen);

                /* Add to the hashtable */
                txn_htable_put(&txn->ht, key, keylen, new_txnd);

                /* Add to the PQueue */
                pqueue_put(&txn->pq, pqd);
        }
}

/**
 * Public functions
 */
int zs_transaction_begin(struct zsdb *db,
                         struct txn **txn,
                         enum TxnType type)
{
        struct txn *t = NULL;
        struct zsdb_priv *priv;
        struct list_head *pos;
        int prio = 0;
        struct txn_data *ftxnd, *atxnd;

        assert_zsdb(db);

        priv = db->priv;
        if (!priv) return ZS_INTERNAL;

        if (!priv->open) {
                zslog(LOGWARNING, "DB `%s` not open!\n", priv->dbdir.buf);
                return ZS_NOT_OPEN;
        }

        t = xcalloc(1, sizeof(struct txn));

        t->db = db;
        t->pq.cmp = iter_pq_cmp;
        txn_htable_init(&t->ht);
        t->datav = NULL;
        t->txn_data_count = 0;
        t->txn_data_alloc = 0;

        /* Add packed files to the iterator */
        list_for_each_forward(pos, &priv->dbfiles.pflist) {
                struct txn_data *ptxnd;
                struct zsdb_file *f;
                unsigned char *key;
                size_t keylen;

                f = list_entry(pos, struct zsdb_file, list);
                ptxnd = txn_data_alloc(ZSDB_BE_PACKED, f->priority, f);
                txn_datav_add_txn(t, ptxnd);

                prio = f->priority;

                zs_packed_file_get_key_from_offset(f, &key, &keylen);

                txn_data_process(t, key, keylen, ptxnd);
        }

        if (type == TXN_PACKED_ONLY)
                goto done;

        /* Add finalised files to the iterator*/
        if (priv->dbfiles.ffcount) {
                prio++;
                ftxnd = txn_data_alloc(ZSDB_BE_FINALISED, prio, priv->fmemtree);
                txn_datav_add_txn(t, ftxnd);
                txn_data_process(t, ftxnd->data.iter->record->key,
                                 ftxnd->data.iter->record->keylen, ftxnd);
        }

        /* Add active file to the iterator */
        prio++;
        atxnd = txn_data_alloc(ZSDB_BE_ACTIVE, prio, priv->memtree);
        txn_datav_add_txn(t, atxnd);
        txn_data_process(t, atxnd->data.iter->record->key,
                         atxnd->data.iter->record->keylen, atxnd);

done:
        *txn = t;

        return ZS_OK;
}

struct txn_data *zs_transaction_get(struct txn *txn)
{
        struct iter_key_data *ikdata = NULL;
        struct txn_data *tdata = NULL;
        struct txn_htable_entry *entry = NULL;

        if (!txn)
                return NULL;

        assert_zsdb(txn->db);

        ikdata = pqueue_get(&txn->pq);
        if (!ikdata)
                return NULL;

        entry = txn_htable_get(&txn->ht, ikdata->key, ikdata->len);
        if (entry) {
                tdata = entry->value;
                entry = txn_htable_remove(&txn->ht, ikdata->key, ikdata->len);
                free_txn_htable_entry(&entry);
        }

        free_iter_key_data(&ikdata);

        return tdata;
}

int zs_transaction_next(struct txn *txn, struct txn_data *data)
{
        if (!txn)
                return 0;

        assert_zsdb(txn->db);

        txn_data_next(txn, data);

        return txn->pq.count ? 1 : 0;
}

void zs_transaction_end(struct txn **txn)
{
        struct txn *t;

        if (txn && *txn) {
                struct iter_key_data *idata;

                t = *txn;
                *txn = NULL;
                t->db = NULL;

                while ((idata = pqueue_get(&t->pq)))
                        free_iter_key_data(&idata);

                pqueue_free(&t->pq);
                txn_htable_free(&t->ht);
                txn_datav_clear_txn(t);

                xfree(t);
        }
}
