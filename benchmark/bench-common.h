/*
 * Common functions used in benchmarks
 */

#ifndef _BENCH_COMMON_H_
#define _BENCH_COMMON_H_

#include <libzeroskip/macros.h>

extern uint64_t get_time_now(void);
extern void print_warnings(void);
extern void print_environment(void);
extern void print_header(void);
#endif  /* _BENCH_COMMON_H_ */
