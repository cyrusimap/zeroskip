/*
 * zeroskip-asserts.c
 *
 * This file is part of zeroskip.
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 */

#include <assert.h>

#include "zeroskip-priv.h"


#ifdef ZS_DEBUG

static void assert_zsdb(struct zsdb *db)
{
}

#else

#define assert_zsdb(x)
#endif
