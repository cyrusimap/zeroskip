/*
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 * Copyright (c) 2018 Partha Susarla <mail@spartha.org>
 *
 */

#include <getopt.h>
#include <sys/param.h>          /* For MAXPATHLEN */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "cmds.h"
#include "zeroskip.h"
#include "cstring.h"

int cmd_new(int argc, char **argv, const char *progname)
{
        static struct option long_options[] = {
                {"config", required_argument, NULL, 'c'},
                {"help", no_argument, NULL, 'h'},
                {NULL, 0, NULL, 0}
        };
        int option;
        int option_index;
        const char *config_file = NULL;
        #if 0
        struct skiplistdb *db = NULL;
        #endif
        struct txn *tid = NULL;
        char *fname;

        while((option = getopt_long(argc, argv, "", long_options, &option_index)) != -1) {
                switch (option) {
                case 'c':       /* config file */
                        config_file = optarg;
                        break;
                case 'h':
                case '?':
                default:
                        cmd_die_usage(progname, cmd_new_usage);
                };
        }

        if (argc - optind != 1) {
                cmd_die_usage(progname, cmd_new_usage);
        }

        fname = argv[optind];

        cmd_parse_config(config_file);

#if 0
        if (skiplistdb_init(type, &db, &tid) != SDB_OK) {
                fprintf(stderr, "Failed initialising.\n");
                exit(EXIT_FAILURE);
        }

        if (skiplistdb_open(fname, db, SDB_CREATE, &tid) != SDB_OK) {
                fprintf(stderr, "Could not create skiplist DB.\n");
                goto fail1;
        }

        if (skiplistdb_close(db) != SDB_OK) {
                fprintf(stderr, "Could not close skiplist DB.\n");
                goto fail1;
        }

        printf("Created %s db %s.\n",
               (type == ZERO_SKIP) ? "zeroskip" : "twoskip",
               fname);

fail1:
        if (skiplistdb_final(db) != SDB_OK) {
                fprintf(stderr, "Failed destroying the database instance.\n");
                exit(EXIT_FAILURE);
        }
#endif

        exit(EXIT_SUCCESS);
}
