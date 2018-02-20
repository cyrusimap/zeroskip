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
#include "log.h"
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
        char *dbname = NULL;
        char *key = NULL;
        char *value = NULL;
        int ret;

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

        dbname = argv[optind++];
        key = argv[optind++];
        value = argv[optind++];

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

        if (zsdb_write_lock_acquire(db, 0) != ZS_OK) {
                zslog(LOGWARNING, "Could not acquire write lock for addition.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_add(db, (unsigned char *)key, strlen(key),
                     (unsigned char *)value, strlen(value)) != ZS_OK) {
                zslog(LOGDEBUG, "Cannot add record to %s\n", dbname);
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_commit(db) != ZS_OK) {
                zslog(LOGDEBUG, "Could not commit record.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        ret = EXIT_SUCCESS;
done:
        if (zsdb_write_lock_release(db) != ZS_OK) {
                zslog(LOGWARNING, "Could not release write lock after addition.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_close(db) != ZS_OK) {
                zslog(LOGWARNING, "Could not close DB.\n");
                ret = EXIT_FAILURE;
        }

        zsdb_final(&db);

        exit(ret);
}
