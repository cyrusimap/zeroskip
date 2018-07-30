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

int cmd_finalise(int argc, char **argv, const char *progname)
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
        int ret;

        while((option = getopt_long(argc, argv, "", long_options, &option_index)) != -1) {
                switch (option) {
                case 'c':       /* Config file */
                        config_file = optarg;
                        break;
                case 'h':
                case '?':
                default:
                        cmd_die_usage(progname, cmd_finalise_usage);
                };
        }

        if (argc - optind != 1) {
                cmd_die_usage(progname, cmd_finalise_usage);
        }

        dbname = argv[optind];

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
                fprintf(stderr, "ERROR: Could not acquire write lock for addition.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_finalise(db) != ZS_OK) {
                fprintf(stderr, "ERROR: Cannot finalised db %s\n", dbname);
                ret = EXIT_FAILURE;
                goto done;
        }


        ret = EXIT_SUCCESS;
        fprintf(stderr, "OK\n");
done:
        if (zsdb_write_lock_release(db) != ZS_OK) {
                fprintf(stderr, "Could not release write lock after addition.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_close(db) != ZS_OK) {
                fprintf(stderr, "Could not close DB.\n");
                ret = EXIT_FAILURE;
        }

        zsdb_final(&db);

        exit(ret);
}
