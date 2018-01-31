/*
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 * Copyright (c) 2018 Partha Susarla <mail@spartha.org>
 *
 */

#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>

#include "cmds.h"
#include "log.h"
#include "zeroskip.h"

int cmd_dump(int argc, char **argv, const char *progname)
{
        static struct option long_options[] = {
                {"config", required_argument, NULL, 'c'},
                {"dump", required_argument, NULL, 'd'},
                {"help", no_argument, NULL, 'h'},
                {NULL, 0, NULL, 0}
        };
        int option;
        int option_index;
        const char *config_file = NULL;
        struct zsdb *db = NULL;
        const char *dbname;
        int ret;
        DBDumpLevel level = DB_DUMP_ACTIVE;

        while((option = getopt_long(argc, argv, "d", long_options, &option_index)) != -1) {
                switch (option) {
                case 'd':       /* level of detail */
                        level = parse_dump_level_string(optarg);
                        break;
                case 'c':       /* config file */
                        config_file = optarg;
                        break;
                case 'h':
                case '?':
                default:
                        cmd_die_usage(progname, cmd_dump_usage);
                };
        }

        if (argc - optind != 1) {
                cmd_die_usage(progname, cmd_dump_usage);
        }

        dbname = argv[optind];

        cmd_parse_config(config_file);

        if (zsdb_init(&db) != ZS_OK) {
                zslog(LOGWARNING, "Failed initialising DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_open(db, dbname, MODE_RDWR) != ZS_OK) {
                zslog(LOGWARNING, "Could not open DB %s.\n", dbname);
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_dump(db, level) != ZS_OK) {
                zslog(LOGWARNING, "Failed dumping records in %s.\n",
                      dbname);
                ret = EXIT_FAILURE;
                goto done;
        }

        ret = EXIT_SUCCESS;
done:
        if (zsdb_close(db) != ZS_OK) {
                zslog(LOGWARNING, "Could not close DB.\n");
                ret = EXIT_FAILURE;
        }

        zsdb_final(&db);

        exit(ret);
}
