/*
 * log.c - logging utils
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */
#include <libzeroskip/log.h>
#include <libzeroskip/macros.h>

#include <stdarg.h>
#include <stdlib.h>
#include <syslog.h>

#define LOGBUF_SIZE  1024

#define ZS_LOG_LEVEL_ENV      "ZS_LOG_LEVEL"
#define ZS_LOG_FILE_ENV       "ZS_LOG_FILE"
#define ZS_LOG_TO_SYSLOG_ENV  "ZS_LOG_TO_SYSLOG"

/* Map from ZS log levels to syslog() levels */
const int syslogLevels[] = {
        LOG_DEBUG,
        LOG_INFO,
        LOG_NOTICE,
        LOG_WARNING,
};

static enum log_level get_log_level(void)
{
        char *log_level;
        enum log_level level;

        log_level = getenv(ZS_LOG_LEVEL_ENV);
        if (!log_level)
                return LOGNONE;

        level = (enum log_level)strtoul(log_level, NULL, 10);

        return level;
}

static char *get_log_file_name(void)
{
        return getenv(ZS_LOG_FILE_ENV);
}

static int should_log_to_syslog(void)
{
        return getenv(ZS_LOG_TO_SYSLOG_ENV) ? 1 : 0;
}

static int _zslog(int level, const char *msg)
{
        FILE *fp;
        char *zs_log_file = NULL;
        int log_to_stdout;
        int ret;

        zs_log_file = get_log_file_name();
        log_to_stdout = (zs_log_file == NULL);

        fp = (log_to_stdout) ? stdout : fopen(zs_log_file, "a");
        if (!fp) return 0;

        ret = fprintf(fp, "[zeroskip] %s", msg);
        fflush(fp);

        if (!log_to_stdout)
                fclose(fp);

        if (should_log_to_syslog())
                syslog(syslogLevels[level], "%s", msg);

        return ret;
}

int zslog(int level, const char *fmt, ...)
{
        va_list ap;
        char msg[LOGBUF_SIZE];
        int log_level;

        log_level = get_log_level();

        if (log_level < level)
                return 0;

        va_start(ap, fmt);
        vsnprintf(msg, sizeof(msg), fmt, ap);
        va_end(ap);

        return _zslog(level, msg);
}
