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
#include <libzeroskip/zeroskip.h>

int cmd_delete(int argc, char **argv, const char *progname)
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
        char *dbname = NULL;
        char *key = NULL;
        int ret;

        while((option = getopt_long(argc, argv, "", long_options, &option_index)) != -1) {
                switch (option) {
                case 'c':
                        config_file = optarg;
                        break;
                case 'h':
                case '?':
                default:
                        cmd_die_usage(progname, cmd_delete_usage);
                };
        }

        if (argc - optind != 2) {
                cmd_die_usage(progname, cmd_delete_usage);
        }

        dbname = argv[optind++];
        key = argv[optind++];

        cmd_parse_config(config_file);

        if (zsdb_init(&db, NULL) != ZS_OK) {
                fprintf(stderr, "ERROR: Failed initialising DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_open(db, dbname, MODE_RDWR) != ZS_OK) {
                fprintf(stderr, "ERROR: Could not open DB %s.\n", dbname);
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_write_lock_acquire(db, 0) != ZS_OK) {
                fprintf(stderr, "ERROR: Could not acquire write lock for deletion.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_remove(db, (unsigned char *)key, strlen(key), NULL) != ZS_OK) {
                fprintf(stderr, "ERROR: Cannot delete record from %s\n", dbname);
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_commit(db, NULL) != ZS_OK) {
                fprintf(stderr, "ERROR: Could not commit record.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_write_lock_release(db) != ZS_OK) {
                fprintf(stderr, "ERROR: Could not release write lock after deletion.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        ret = EXIT_SUCCESS;
        fprintf(stderr, "OK\n");
done:
        if (zsdb_close(db) != ZS_OK) {
                fprintf(stderr, "ERROR: Could not close DB.\n");
                ret = EXIT_FAILURE;
        }

        zsdb_final(&db);

        exit(ret);
}
