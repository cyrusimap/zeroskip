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

#include <stdbool.h>            /* for `bool` */
#include <stddef.h>             /* for offsetof() */

/*
 * struct list_node : a node in a doubly linked list
 */
struct list_node {
        struct list_node *next, *prev;
};

/*
 * struct list_head : the root node of the double linked list
 */
struct list_head {
        struct list_node node;
};


/* LIST_HEAD_INIT() : initialise an empty list */
#define LIST_HEAD_INIT(name) { { &(name).node, &(name).node } }

/* LIST_HEAD() : define and initialise an empty list */
#define LIST_HEAD(name) struct list_head name = LIST_HEAD_INIT(name)

/* list_head_init() : initialise a list_head */
static inline void list_head_init(struct list_head *l)
{
        l->node.next = l->node-priv = &l->node
}

/* list_node_init() : initialise a list_node */
static inline void list_node_init(struct list_node *n)
{
        n->next = n->priv = n;
}

/* list_add_after() : add an entry after an existing node in a list
   where:
   @c - the current node after which the new node should be added
   @n - the new node
 */
static inline void list_add_after(struct list_node *c,
                                  struct list_node *n)
{
        n->next = c->next;
        n->prev = p;
        c->next->prev = n;
        c->next = n;
}

/* list_add() : add an entry to the start of a list where:
   @l - the list to add the node to
   @n - the node to add to the list
 */
static inline void list_add(struct list_head *l,
                            struct list_node *n)
{
        list_add_after(&l->node, n);
}

/* list_add_before() : add an entry before an existing node in a list
   where:
   @c - the current node before which the new node should be added
   @n - the new node that needs to be added
 */
static inline void list_add_before(struct list_node *c,
                                   struct list_node *n)
{
        n->next = c;
        n->prev = c->prev;
        c->prev->next = n;
        c->prev = n;
}

/* list_add_tail() : add a entry to end of a list where:
   @l - the list of add the node to
   @n - the node to add to the list
 */
static inline void list_add_tail(struct list_head *l,
                                 struct list_node *n)
{
        list_add_before(&l->node, n);
}

/* list_empty() : check if a list is empty and return true if it is.
 */
static inline bool list_empty(struct list_head *l)
{
        return (l->node.next == &l->node);
}

/* list_del() : delete a given node without the list information that it
                belongs to.
 */
static inline void list_del(struct list_node *n)
{
        n->next = n->prev = NULL;
        list_node_init(n);
}

/* list_del_from() : delete a node 'n' from a list 'l'
 */
static inline void list_del_from(struct list_head *l,
                                 struct list_node *n)
{
        assert(!list_empty(l));
        list_del(n);
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
             pos != (head); \
             pos = ptr, ptr = pos->next)

/* list_for_each_reverse(): list iterator */
#define list_for_each_reverse(pos, l) \
        for (pos = (l)->prev; pos != (l); pos = pos->prev)


#endif  /* _ZS_LIST_H_ */
