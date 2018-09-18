/* zsbench - A tool to benchmark Zeroskip. This is based on the db_bench tool
 *           in LevelDB.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <libgen.h>

#include <libzeroskip/macros.h>
#include <libzeroskip/strarray.h>
#include <libzeroskip/util.h>
#include <libzeroskip/version.h>
#include <libzeroskip/zeroskip.h>

/* Globals */
static const char *DBNAME;
static const char *BENCHMARKS;

static struct option long_options[] = {
        {"benchmarks", required_argument, NULL, 'b'},
        {"db", required_argument, NULL, 'd'},
        {"help", no_argument, NULL, 'h'},
        {NULL, 0, NULL, 0}
};

static void usage(const char *progname)
{
        printf("Usage: %s [OPTION]... [DB]...\n", progname);

        printf("  -b, --benchmarks     list of benchmarks to run\n");
        printf("                       writeseq,writerandom,\n");
        printf("  -d, --db             the db to run the benchmarks on\n");
        printf("  -h, --help           display this help and exit\n");
}

static void do_write_seq(void)
{
}

static void do_write_random(void)
{
}

static int parse_options(int argc, char **argv, const struct option *options)
{
        int option;
        int option_index;

        while ((option = getopt_long(argc, argv, "d:b:h?",
                                     long_options, &option_index)) != -1) {
                switch (option) {
                case 'b':
                        BENCHMARKS = optarg;
                        break;
                case 'd':
                        DBNAME = optarg;
                        break;
                case 'h':
                        _fallthrough_;
                case '?':
                        usage(basename(argv[0]));
                        return option == 'h';
                }
        }

        return 0;
}

static int run_benchmarks(void)
{
        struct str_array benchmarks;
        int i;

        str_array_from_strsplit(&benchmarks, BENCHMARKS, ',');

        for (i = 0; i < benchmarks.count; i++) {
                if (strcmp(benchmarks.datav[i], "writeseq") == 0) {
                        do_write_seq();
                } else if (strcmp(benchmarks.datav[i], "writerandom") == 0) {
                        do_write_random();
                } else {
                        fprintf(stderr, "Unknown benchmark '%s'\n",
                                benchmarks.datav[i]);
                }
        }

        str_array_clear(&benchmarks);
        return 0;
}

int main(int argc, char **argv)
{
        int ret = EXIT_SUCCESS;

        if (argc == 0) {
                usage(basename(argv[0]));
                ret = EXIT_FAILURE;
                goto done;
        }

        ret = parse_options(argc, argv, long_options);

        if (DBNAME == NULL) {
        }

        if (BENCHMARKS != NULL) {
                ret = run_benchmarks();
        } else {
                fprintf(stderr, "No benchmarks specified.\n");
                usage(basename(argv[0]));
                ret = EXIT_FAILURE;
                goto done;
        }

done:
        exit(ret);
}
