/*
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 * Copyright (c) 2018 Partha Susarla <mail@spartha.org>
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <libzeroskip/macros.h>
#include <libzeroskip/zeroskip.h>

void cmd_die_usage(const char *progname, const char *usage)
{
        fprintf(stderr, "Usage: %s %s\n", progname, usage);
        exit(EXIT_FAILURE);
}


int cmd_parse_config(const char *cfile _unused_)
{
        return 0;
}

DBDumpLevel parse_dump_level_string(const char *dblevel)
{
        if (!dblevel || strcmp(dblevel, "active") == 0)
                return DB_DUMP_ACTIVE;
        else if (strcmp(dblevel, "all") == 0)
                return DB_DUMP_ALL;
        else {
                fprintf(stderr, "Invalid dump level, should be `active or `all`\n");
                exit(EXIT_FAILURE);
        }

        return DB_DUMP_ALL;           /* Default to dump all */
}
