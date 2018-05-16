/*
 * log.h - logging utils
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#ifndef _LOG_H_
#define _LOG_H_

#include "cstring.h"
#include "macros.h"

#include <stdio.h>

CPP_GUARD_START

enum {
        LOGDEBUG   = 0,
        LOGVERBOSE = 1,
        LOGNOTICE  = 2,
        LOGWARNING = 3,
};

extern int zs_log_verbosity;
extern int zs_log_to_syslog;   /* Set to 1 to enable logging to syslog */
extern cstring zs_log_file;

int zslog(int level, const char *fmt, ...);

CPP_GUARD_END

#endif  /* _LOG_H_ */
