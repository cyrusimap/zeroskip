/*
 * log.c - logging utils
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */
#include "log.h"
#include "macros.h"

#include <stdarg.h>
#include <syslog.h>

#define LOGBUF_SIZE  1024

 /* TODO: These should go into a global structure. */
int zs_log_verbosity = LOGDEBUG;
int zs_log_to_syslog = 0;
cstring zs_log_file = CSTRING_INIT;

/* Map from ZS log levels to syslog() levels */
const int syslogLevels[] = {
        LOG_DEBUG,
        LOG_INFO,
        LOG_NOTICE,
        LOG_WARNING,
};

static int _zslog(int level, const char *msg)
{
        FILE *fp;
        int log_to_stdout = zs_log_file.buf == cstring_base;
        int ret;

        if (level < zs_log_verbosity)
                return 0;

        fp = (log_to_stdout) ? stdout : fopen(zs_log_file.buf, "a");
        if (!fp) return 0;

        ret = fprintf(fp, "[zeroskip] %s", msg);
        fflush(fp);

        if (!log_to_stdout)
                fclose(fp);

        if (zs_log_to_syslog)
                syslog(syslogLevels[level], "%s", msg);

        return ret;
}

int zslog(int level, const char *fmt, ...)
{
        va_list ap;
        char msg[LOGBUF_SIZE];

        if (level < zs_log_verbosity)
                return 0;

        va_start(ap, fmt);
        vsnprintf(msg, sizeof(msg), fmt, ap);
        va_end(ap);

        return _zslog(level, msg);
}
