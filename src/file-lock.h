/*
 * file-lock.h
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#ifndef _ZS_FILE_LOCK_H_
#define _ZS_FILE_LOCK_H_

#include "cstring.h"

struct file_lock {
        cstring fname;
        int locked;
        int fd;
};

#define FILE_LOCK_INIT  { CSTRING_INIT, 0 }

int file_lock_acquire(struct file_lock *lk, const char *path,
                      const char *fname, long timeout_ms);
int file_lock_release(struct file_lock *lk);

static inline int file_lock_is_locked(struct file_lock *lk)
{
        return (lk->locked == 1);
}


#endif  /* _ZS_FILE_LOCK_H_ */
