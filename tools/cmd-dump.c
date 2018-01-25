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
        const char *fname;
        DBDumpLevel level = DB_DUMP_ALL;

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

        fname = argv[optind];

        fprintf(stderr, "Opening db: %s\n", fname);

        cmd_parse_config(config_file);

#if 0
        if (skiplistdb_init(type, &db, &tid) != SDB_OK) {
               fprintf(stderr, "Failed initialising.\n");
               exit(EXIT_FAILURE);
        }

        if (skiplistdb_open(fname, db, SDB_CREATE, &tid) != SDB_OK) {
                fprintf(stderr, "Could not open skiplist DB.\n");
                goto fail1;
        }

        if (skiplistdb_dump(db, level) != SDB_OK) {
                fprintf(stderr, "Cannot dump db %s\n", fname);
                goto fail1;
        }

        if (skiplistdb_close(db) != SDB_OK) {
                fprintf(stderr, "Could not close skiplist DB.\n");
                goto fail1;
        }

fail1:
        if (skiplistdb_final(db) != SDB_OK) {
                fprintf(stderr, "Failed destroying the database instance.\n");
                exit(EXIT_FAILURE);
        }
#endif

        exit(EXIT_SUCCESS);
}
