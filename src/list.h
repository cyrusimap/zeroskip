/*
 * list.h
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#ifndef _ZS_LIST_H_
#define _ZS_LIST_H_

#include <assert.h>
#include <stdbool.h>            /* for `bool` */
#include <stddef.h>             /* for offsetof() */

#include "macros.h"

CPP_GUARD_START
/*
 * struct list_head : the root node of the double linked list
 */
struct list_head {
        struct list_head *prev, *next;
};


/* LIST_HEAD() : define and initialise an empty list */
#define LIST_HEAD(name) struct list_head name = { &(name), &(name) }

/* list_head_init() : initialise a new list */
static inline void list_head_init(struct list_head *l)
{
        l->next = l->prev = l;
}

/* list_add_head() : add an entry at the head of the list
   where:
   @n - the new node
   @h - the list head
 */
static inline void list_add_head(struct list_head *n,
                                 struct list_head *h)
{
        h->next->prev = n;
        n->next = h->next;
        n->prev = h;
        h->next = n;
}

/* list_add_tail() : add an entry to the tail of the lsit
   @n - the new node
   @h - the list head
 */
static inline void list_add_tail(struct list_head *n,
                                 struct list_head *h)
{
        h->prev->next = n;
        n->next = h;
        n->prev = h->prev;
        h->prev = n;
}

/* list_del() : delete a given node.
 */
static inline void _list_del(struct list_head *prev, struct list_head *next)
{
        next->prev = prev;
        prev->next = next;
}

static inline void list_del(struct list_head *e)
{
        _list_del(e->prev, e->next);
}

static inline void list_del_init(struct list_head *e)
{
        list_del(e);
        list_head_init(e);
}

/* list_replace() : replace an existing entry in the list
 */
static inline void list_replace(struct list_head *cur, struct list_head *n)
{
        n->next = cur->next;
        n->prev = cur->prev;
        n->prev->next = n;
        n->next->prev = n;
}

/* list_empty() : check if a list is empty and return true if it is.
 */
static inline bool list_empty(struct list_head *l)
{
        return (l == l->next);
}

/* list_entry() : convert the list_node back to the structure containing it,
   where:
   @n - the node
   @type - the type of the entry
   @member - the list_node member of the type
 */
#define list_entry(n, type, member) \
        ((type *) ((char *) (n) - offsetof(type, member)))

/* list_first(): get the first entry of the list */
#define list_first(l, type, member) \
        list_entry((l)->next, type, member)

/* list_for_each_forward(): list iterator */
#define list_for_each_forward(pos, l) \
        for (pos = (l)->next; pos != (l); pos = pos->next)

/* list_for_each_forward_safe(): list iterator, where entries can be removed */
#define list_for_each_forward_safe(pos, ptr, l) \
        for (pos = (l)->next, ptr = pos->next; \
             pos != (l); \
             pos = ptr, ptr = pos->next)

/* list_for_each_reverse(): list iterator */
#define list_for_each_reverse(pos, l) \
        for (pos = (l)->prev; pos != (l); pos = pos->prev)

/* list_for_each_reverse_safe(): list iterator, where entries can be removed safely */
#define list_for_each_reverse_safe(pos, ptr, l) \
        for (pos = (l)->prev; ptr = pos->prev;  \
             pos != (l);                        \
             pos = ptr, ptr = pos->prev)

CPP_GUARD_END
#endif  /* _ZS_LIST_H_ */
