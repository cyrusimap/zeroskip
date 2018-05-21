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

#include <libzeroskip/macros.h>

#include <stdio.h>

CPP_GUARD_START

enum log_level {
        LOGNONE    = -1,
        LOGWARNING = 0,
        LOGNOTICE  = 1,
        LOGVERBOSE = 2,
        LOGDEBUG   = 3,
};

int zslog(int level, const char *fmt, ...);

CPP_GUARD_END

#endif  /* _LOG_H_ */
