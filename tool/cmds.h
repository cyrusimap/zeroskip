/*
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 * Copyright (c) 2018 Partha Susarla <mail@spartha.org>
 *
 */

#ifndef _CMDS_H_
#define _CMDS_H_

#include <libzeroskip/zeroskip.h>

extern int cmd_batch(int argc, char **argv, const char *progname);
#define cmd_batch_usage "batch "

extern int cmd_bindump(int argc, char **argv, const char *progname);
#define cmd_bindump_usage "bindump -f <db-file>"

extern int cmd_consistent(int argc, char **argv, const char *progname);
#define cmd_consistent_usage "consistent "

extern int cmd_damage(int argc, char **argv, const char *progname);
#define cmd_damage_usage "damage "

extern int cmd_delete(int argc, char **argv, const char *progname);
#define cmd_delete_usage "delete [--config CONFIGFILE] DB <key>"

extern int cmd_dump(int argc, char **argv, const char *progname);
#define cmd_dump_usage "dump [--config CONFIGFILE] [--recs=active|all] DB"

extern int cmd_finalise(int argc, char **argv, const char *progname);
#define cmd_finalise_usage "finalise DB"

extern int cmd_get(int argc, char **argv, const char *progname);
#define cmd_get_usage "get [--config CONFIGFILE] DB <key>"

extern int cmd_info(int argc, char **argv, const char *progname);
#define cmd_info_usage "info DB"

extern int cmd_repack(int argc, char **argv, const char *progname);
#define cmd_repack_usage "repack [--config CONFIGFILE] DB"

extern int cmd_set(int argc, char **argv, const char *progname);
#define cmd_set_usage "set [--config CONFIGFILE] DB <key> <value>"

extern int cmd_show(int argc, char **argv, const char *progname);
#define cmd_show_usage "show [--config CONFIGFILE] DB <prefix>"

extern int cmd_new(int argc, char **argv, const char *progname);
#define cmd_new_usage "new [--config CONFIGFILE] DB"

extern void cmd_die_usage(const char *progname, const char *usage);

extern int cmd_parse_config(const char *cfile);

extern DBDumpLevel parse_dump_level_string(const char *dblevel);
#endif  /* _CMDS_H_ */
