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
#include <libzeroskip/mappedfile.h>
#include <libzeroskip/zeroskip.h>

static void usage_and_die(const char *progname)
{
         fprintf(stderr, "Usage: %s %s\n", progname, cmd_bindump_usage);
         fprintf(stderr, "Dump the contents of a specific file in a Zeroskip DB.\n");
         fprintf(stderr, "Use in combination with `zeroskip info`.\n");
         fprintf(stderr, "\n");
         fprintf(stderr, "-f, --file       db file to dump\n");
         fprintf(stderr, "-h, --help       this help\n");
         exit(EXIT_FAILURE);
}

static int bindump(struct mappedfile *mf)
{
        size_t pos = 0;

        for (pos = 0; pos < mf->size; pos++){
                printf("%08x\n", mf->ptr[pos]);
        }

        printf("\n");

        return 0;
}

int cmd_bindump(int argc, char **argv, const char *progname)
{
        static struct option long_options[] = {
                {"file", required_argument, NULL, 'f'},
                {"help", no_argument, NULL, 'h'},
                {NULL, 0, NULL, 0}
        };
        int option;
        int option_index;
        const char *dbfname = NULL;
        struct mappedfile *mf = NULL;
        int ret;

        while((option = getopt_long(argc, argv, "f:h?", long_options,
                                    &option_index)) != -1) {
                switch (option) {
                case 'f':
                        dbfname = optarg;
                        break;
                case 'h':
                case '?':
                default:
                        usage_and_die(progname);
                };
        }

        if (dbfname == NULL)
                usage_and_die(progname);


        if (mappedfile_open(dbfname, MAPPEDFILE_RD, &mf) != 0) {
                fprintf(stderr, "ERROR: Error opening %s.\n",
                      dbfname);
                ret = EXIT_FAILURE;
                goto done;
        }

        if (bindump(mf) != ZS_OK) {
                fprintf(stderr, "ERROR: Failed dumping %s in binary.\n",
                      dbfname);
                ret = EXIT_FAILURE;
                goto done;
        }

        ret = EXIT_SUCCESS;
done:
        mappedfile_close(&mf);

        exit(ret);
}
