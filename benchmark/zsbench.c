/* zsbench - A tool to benchmark Zeroskip. This is based on the db_bench tool
 *           in LevelDB.
 */

#include <getopt.h>
#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <libzeroskip/cstring.h>
#include <libzeroskip/macros.h>
#include <libzeroskip/strarray.h>
#include <libzeroskip/util.h>
#include <libzeroskip/version.h>
#include <libzeroskip/zeroskip.h>

/* Globals */
static const char *DBNAME;
static const char *BENCHMARKS;
static int NUMRECS = 1000;

static struct option long_options[] = {
        {"benchmarks", required_argument, NULL, 'b'},
        {"db", required_argument, NULL, 'd'},
        {"numrecs", optional_argument, NULL, 'n'},
        {"help", no_argument, NULL, 'h'},
        {NULL, 0, NULL, 0}
};

static void usage(const char *progname)
{
        printf("Usage: %s [OPTION]... [DB]...\n", progname);

        printf("  -b, --benchmarks     comma separated list of benchmarks to run\n");
        printf("                       Available benchmarks:\n");
        printf("                       * writeseq    - write values in sequential key order\n");
        printf("                       * writerandom - write values in random key order\n");
        printf("\n");
        printf("  -d, --db             the db to run the benchmarks on\n");
        printf("  -n, --numrecs        number of records to write[default: 1000]\n");
        printf("  -h, --help           display this help and exit\n");
}

static void print_warnings(void)
{
}

static void print_environment(void)
{
        fprintf(stderr, "Zeroskip:       version %s\n", ZS_VERSION);

#if defined(LINUX)
        time_t now = time(NULL);
        fprintf(stderr, "Date:           %s", ctime(&now));;

        FILE *cpuinfo = fopen("/proc/cpuinfo", "r");
        if (cpuinfo != NULL) {
                char line[1000];
                int num_cpus = 0;
                cstring cpu_type;
                cstring cache_size;

                cstring_init(&cpu_type, 0);
                cstring_init(&cache_size, 0);

                while (fgets(line, sizeof(line), cpuinfo) != NULL) {
                        const char *sep = strchr(line, ':');
                        cstring key, val;

                        if (sep == NULL)
                                continue;

                        cstring_init(&key, 0);
                        cstring_init(&val, 0);

                        cstring_add(&key, line, (sep - 1 - line));
                        cstring_trim(&key);

                        cstring_addstr(&val, (sep + 1));
                        cstring_trim(&val);

                        if (strcmp("model name", key.buf) == 0) {
                                ++num_cpus;
                                cstring_release(&cpu_type);
                                cstring_addstr(&cpu_type, val.buf);
                        } else if (strcmp("cache size", key.buf) == 0) {
                                cstring_release(&cache_size);
                                cstring_addstr(&cache_size, val.buf);
                        }

                        cstring_release(&key);
                        cstring_release(&val);
                }

                fclose(cpuinfo);

                fprintf(stderr, "CPU:            %d * [%s]\n",
                        num_cpus, cpu_type.buf);
                fprintf(stderr, "CPUCache:       %s\n", cache_size.buf);

                cstring_release(&cpu_type);
                cstring_release(&cache_size);
        }
#endif
}

static void print_header(void)
{
        print_environment();
        print_warnings();
        fprintf(stdout, "------------------------------------------------\n");
}


static void do_write_seq(void)
{
        int i;

        for (i = 0; i < NUMRECS; i++) {
                char key[100];
                snprintf(key, sizeof(key), "%016d", i);
                /* printf(">> %s\n", key); */
        }
}

static void do_write_random(void)
{
        printf("do_write_random\n");
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
                case 'n':
                        NUMRECS = atoi(optarg);
                        break;
                case 'h':
                        _fallthrough_;
                case '?':
                        usage(basename(argv[0]));
                        exit(option == 'h');
                }
        }

        return 0;
}

static int run_benchmarks(void)
{
        struct str_array benchmarks;
        int i;

        print_header();

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
                ret = EXIT_FAILURE;
                goto done;
        }

done:
        exit(ret);
}
