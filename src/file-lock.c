/*
 * file-lock.c
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#include "file-lock.h"
#include "log.h"
#include "util.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define FILE_LOCK_FNAME       "lock"
#define FILE_LOCK_SUFFIX      ".lock"
#define FILE_LOCK_SUFFIX_LEN  5

/* Internal functions */
static int create_lock_file(const char *path)
{
        if (!path) {
                fprintf(stderr, "Invalid path!\n");
                return -1;
        }

        return open(path, O_RDWR | O_CREAT | O_EXCL, 0666);
}

static int remove_lock_file(const char *path)
{
        zslog(LOGDEBUG, "Removing lock file %s\n",
              path);
        if (!path) {
                fprintf(stderr, "Invalid path!\n");
                return -1;
        }

        return unlink(path);
}

static int flock_lock(struct file_lock *lk, const char *path,
                      const char *fname)
{
        cstring_release(&lk->fname);

        cstring_addstr(&lk->fname, path);
        cstring_addch(&lk->fname, '/');
        if (fname)
                cstring_addstr(&lk->fname, fname);
        else
                cstring_addstr(&lk->fname, FILE_LOCK_FNAME);

        cstring_addstr(&lk->fname, FILE_LOCK_SUFFIX);

        zslog(LOGDEBUG, "Creating lock file %s\n",
              lk->fname.buf);

        lk->fd = create_lock_file(lk->fname.buf);
        if (lk->fd != -1)
                lk->locked = 1;

        return lk->fd;
}

#define BACKOFF_INITIAL_MS 1L
#define BACKOFF_MULT_FACTOR_MAX 1000
static int flock_with_timeout(struct file_lock *lk,
                              const char *path,
                              const char *fname,
                              long timeout_ms)
{
        int multiplier = 1, n = 1;
        long remaining_ms = 0;
        static int random_init = 0;

        if (timeout_ms == 0)
                return flock_lock(lk, path, fname);

        if (!random_init) {
                srand((unsigned int)getpid());
        }

        if (timeout_ms > 0)
                remaining_ms = timeout_ms;

        while (1) {
                long backoff_ms, wait_ms;
                int fd;

                fd = flock_lock(lk, path, fname);

                if (fd > 0)
                        return fd;
                else if (errno != EEXIST)
                        return -1; /* some failure */
                else if (timeout_ms > 0 && remaining_ms <= 0)
                        return -1; /* timeout */

                backoff_ms = multiplier * BACKOFF_INITIAL_MS;
                wait_ms = (750 + rand() % 500) * backoff_ms / 1000;
                sleep_ms(wait_ms);
                remaining_ms -= wait_ms;

                multiplier += 2*n + 1;
                if (multiplier > BACKOFF_MULT_FACTOR_MAX)
                        multiplier = BACKOFF_MULT_FACTOR_MAX;
                else
                        n++;
        } /* while */
}

/* Public functions */
int file_lock_acquire(struct file_lock *lk, const char *path,
                      const char *fname, long timeout_ms)
{
        return flock_with_timeout(lk, path, fname, timeout_ms);
}

int file_lock_release(struct file_lock *lk)
{
        int ret;

        if (!lk)
                return -1;

        if (lk->fd < 0 || !lk->locked)
                return 0;

        ret = close(lk->fd);

        remove_lock_file(lk->fname.buf);
        cstring_release(&lk->fname);
        lk->locked = 0;

        return ret ? -1 : 0;
}
