/*
 *
 * Copyright (c) 2018 Partha Susarla <mail@spartha.org>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <libgen.h>

#include <libzeroskip/macros.h>
#include <libzeroskip/util.h>
#include <libzeroskip/version.h>
#include <libzeroskip/zeroskip.h>

#include "cmds.h"

#define DBCMD(name) { #name, cmd_##name##_usage, cmd_##name }
static struct {
        const char *name;
        const char *usage;
        int (*cmd)(int, char **, const char *);
} commands[] = {
        DBCMD(batch),
        DBCMD(consistent),
        DBCMD(damage),
        DBCMD(delete),
        DBCMD(dump),
        DBCMD(finalise),
        DBCMD(get),
        DBCMD(info),
        DBCMD(new),
        DBCMD(repack),
        DBCMD(show),
        DBCMD(set),
};
#undef DBCMD

static const char *progname = NULL;

static void version(void)
{
        fprintf(stderr, "Zeroskip DB tool v" ZS_VERSION "\n");
}

static void usage(void)
{
        size_t i;

        printf("Usage:\n");
        printf("  %s {--help|--version}\n", progname);

        for (i = 0; i < ARRAY_SIZE(commands); i++) {
                printf("  %s %s\n", progname, commands[i].usage);
        }
}

static int global_options(int argc, char **argv)
{
        static struct option long_options[] = {
                {"version", no_argument, NULL, 'v'},
                {"help", no_argument, NULL, 'h'},
                {NULL, 0, NULL, 0}
        };
        int option;
        int option_index;

        while((option = getopt_long(argc, argv, "vh", long_options, &option_index)) != -1) {
                switch (option) {
                case 'v':
                        version();
                        return 0;
                case 'h':
                        version();
                        printf("\n");
                        _fallthrough_;
                case '?':
                        usage();
                        return option == 'h';
                }
        }

        usage();

        return 1;
}

static int process_command(int argc, char **argv)
{
        size_t i;

        for (i = 0; i < ARRAY_SIZE(commands); i++) {
                if (argc && !strcmp(argv[0], commands[i].name))
                        return commands[i].cmd(argc, argv, progname);
        }

        usage();

        return 1;
}

int main(int argc, char **argv)
{
        progname = basename(argv[0]);

        if (argc >= 2 && argv[1][0] != '-')
                return process_command(argc - 1, argv + 1);

        return global_options(argc, argv);
}

