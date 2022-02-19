/*
 * crc32bench - benchmarking crc32 functions that we have with the one in zlib
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>
#include <libzeroskip/crc32c.h>

#include "bench-common.h"

/* Globals */
const char *buf = "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.";

static int NUM_RUNS = 65536;

static struct option long_options[] = {
        {"hardware", required_argument, NULL, 'H'},
        {"sofrware", required_argument, NULL, 'S'},
        {"zlib", optional_argument, NULL, 'Z'},
        {"help", no_argument, NULL, 'h'},
        {NULL, 0, NULL, 0}
};

static void usage(const char *progname)
{
        printf("Usage: %s [OPTION]...\n", progname);

        printf("  -H, --hardware       run with Intel's CPU instructions\n");
        printf("  -S, --software       run with software implementation\n");
        printf("  -D, --default        run with implementation that system decides is correct\n");
        printf("  -Z, --zlib           run with Zlib's implementation\n");
        printf("  -h, --help           display this help and exit\n");
}

static int run_hardware(void)
{
        uint64_t start, finish;
        uint32_t bytes = 0;

        start = get_time_now();
        for (int i = 0; i < NUM_RUNS; i++) {
                bytes += crc32c_hw(0, buf, strlen(buf));
        }
        finish = get_time_now();

        fprintf(stderr, "crc32c_hw       : %u bytes in %" PRIu64 " μs.\n",
                bytes, (finish - start));
        fprintf(stdout, "------------------------------------------------\n");
        return 0;
}

static int run_software(void)
{
        uint64_t start, finish;
        uint32_t bytes = 0;

        start = get_time_now();
        for (int i = 0; i < NUM_RUNS; i++) {
                bytes += crc32c_hw(0, buf, strlen(buf));
        }
        finish = get_time_now();

        fprintf(stderr, "crc32c_sw       : %u bytes in %" PRIu64 " μs.\n",
                bytes, (finish - start));
        fprintf(stdout, "------------------------------------------------\n");
        return 0;
}

static int run_default(void)
{
        uint64_t start, finish;
        uint32_t bytes = 0;

        start = get_time_now();
        for (int i = 0; i < NUM_RUNS; i++) {
                bytes += crc32c(0, buf, strlen(buf));
        }
        finish = get_time_now();

        fprintf(stderr, "crc32c(default) : %u bytes in %" PRIu64 " μs.\n",
                bytes, (finish - start));
        fprintf(stdout, "------------------------------------------------\n");
        return 0;
}

static int run_zlib(void)
{
        uint64_t start, finish;
        uint32_t bytes = 0;

        start = get_time_now();
        for (int i = 0; i < NUM_RUNS; i++) {
                bytes += crc32c(0, buf, strlen(buf));
        }
        finish = get_time_now();

        fprintf(stderr, "crc32(zlib)     : %u bytes in %" PRIu64 " μs.\n",
                bytes, (finish - start));
        fprintf(stdout, "------------------------------------------------\n");

        return 0;
}

static int parse_options_and_run(int argc, char **argv,
                                 const struct option *options)
{
        int option;
        int option_index;
        int header_printed = 0;

        while ((option = getopt_long(argc, argv, "HSDZh?",
                                     long_options, &option_index)) != -1) {
                if (!header_printed && (option != 'h' || option != '?')) {
                        header_printed = 1;
                        print_header();
                        fprintf(stderr, "String Length:  %zu\n", strlen(buf));
                        fprintf(stderr, "Runs:           %d\n", NUM_RUNS);
                        fprintf(stdout, "------------------------------------------------\n");
                }

                switch (option) {
                case 'H':
                        run_hardware();
                        break;
                case 'S':
                        run_software();
                        break;
                case 'D':
                        run_default();
                        break;
                case 'Z':
                        run_zlib();
                        break;
                case 'h':
                case '?':
                        usage(basename(argv[0]));
                        exit(option == 'h');
                }
        }

        return 0;
}

int main(int argc, char **argv)
{
        int ret = EXIT_SUCCESS;

        ret = parse_options_and_run(argc, argv, long_options);
        exit(ret);
}
