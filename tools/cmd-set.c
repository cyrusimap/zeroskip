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
#include <string.h>

#include "cmds.h"
#include "zeroskip.h"

int cmd_set(int argc, char **argv, const char *progname)
{
        static struct option long_options[] = {
                {"config", required_argument, NULL, 'c'},
                {"help", no_argument, NULL, 'h'},
                {NULL, 0, NULL, 0}
        };
        int option;
        int option_index;
        const char *config_file = NULL;
        struct zsdb *db = NULL;
        char *fname = NULL;
        char *key = NULL;
        char *value = NULL;

        while((option = getopt_long(argc, argv, "", long_options, &option_index)) != -1) {
                switch (option) {
                case 'c':       /* Config file */
                        config_file = optarg;
                        break;
                case 'h':
                case '?':
                default:
                        cmd_die_usage(progname, cmd_set_usage);
                };
        }

        if (argc - optind != 3) {
                cmd_die_usage(progname, cmd_set_usage);
        }

        fname = argv[optind++];
        key = argv[optind++];
        value = argv[optind++];

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

        if (skiplistdb_add(db, (unsigned char *)key, strlen(key),
                           (unsigned char *)value, strlen(value),
                           &tid) != SDB_OK) {
                fprintf(stderr, "Cannot add keyval to %s\n", fname);
                goto fail1;
        }

        if (skiplistdb_commit(db, &tid) != SDB_OK) {
                fprintf(stderr, "Could not commit record.\n");
                goto fail1;
        }

        if (skiplistdb_close(db) != SDB_OK) {
                fprintf(stderr, "Could not close skiplist DB.\n");
                goto fail1;
        }

        printf("Set %s to %s in %s\n",
               key, value, fname);
fail1:
        if (skiplistdb_final(db) != SDB_OK) {
                fprintf(stderr, "Failed destroying the database instance.\n");
                exit(EXIT_FAILURE);
        }
        #endif

        exit(EXIT_SUCCESS);
}
