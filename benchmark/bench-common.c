/*
 * Common functions used in benchmarks
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>
#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>

#include "bench-common.h"

#include <libzeroskip/cstring.h>
#include <libzeroskip/strarray.h>
#include <libzeroskip/version.h>

uint64_t get_time_now(void)
{
        struct timeval tv;
        gettimeofday(&tv, NULL);

        return tv.tv_sec * 1000000 + tv.tv_usec;
}

void print_warnings(void)
{
}

void print_environment(void)
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

void print_header(void)
{
        print_environment();
        print_warnings();
        fprintf(stdout, "------------------------------------------------\n");
}
