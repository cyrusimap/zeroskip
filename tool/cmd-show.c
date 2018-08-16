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
#include <libzeroskip/util.h>
#include <libzeroskip/zeroskip.h>

static void usage_and_die(const char *progname)
{
         fprintf(stderr, "Usage: %s %s\n", progname, cmd_show_usage);


        exit(EXIT_FAILURE);
}

static int print_cb(void *data _unused_,
                    const unsigned char *key, size_t keylen,
                    const unsigned char *val, size_t vallen)
{
        size_t i;

        for (i = 0; i < keylen; i++)
                printf("%c", key[i]);

        printf(" : ");

        for (i = 0; i < vallen; i++)
                printf("%c", val[i]);

        printf("\n");

        return 0;
}

int cmd_show(int argc, char **argv, const char *progname)
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
        char *prefix = NULL;
        size_t prefixlen = 0;
        int ret;
        struct zsdb_txn *txn = NULL;

        while((option = getopt_long(argc, argv, "c:h?", long_options,
                                    &option_index)) != -1) {
                switch (option) {
                case 'c':
                        config_file = optarg;
                        break;
                case 'h':
                case '?':
                default:
                        usage_and_die(progname);
                };
        }

        if (argc - optind != 2) {
                usage_and_die(progname);
        }

        dbname = argv[optind++];
        prefix = argv[optind++];
        prefixlen = strlen(prefix);

        cmd_parse_config(config_file);

        if (zsdb_init(&db, NULL, NULL) != ZS_OK) {
                fprintf(stderr, "ERROR: Failed initialising DB.\n");
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_open(db, dbname, MODE_RDWR) != ZS_OK) {
                fprintf(stderr, "ERROR: Could not open DB %s.\n", dbname);
                ret = EXIT_FAILURE;
                goto done;
        }

        if (zsdb_forone(db, (const unsigned char *)prefix,
                        prefixlen, NULL, print_cb, NULL, &txn) != ZS_OK) {
                fprintf(stderr, "ERROR: forone failed!\n");
                ret = EXIT_FAILURE;
                goto fail1;
        }

        if (zsdb_foreach(db, (const unsigned char *)prefix,
                         prefixlen, NULL, print_cb, NULL, &txn) != ZS_OK) {
                fprintf(stderr, "ERROR: forone failed!\n");
                ret = EXIT_FAILURE;
                goto fail1;
        }

        fprintf(stderr, "\n");

        ret = EXIT_SUCCESS;
fail1:
        zsdb_transaction_end(&txn);
done:
        if (zsdb_close(db) != ZS_OK) {
                fprintf(stderr, "ERROR: Could not close DB.\n");
                ret = EXIT_FAILURE;
        }

        zsdb_final(&db);

        exit(ret);
}
