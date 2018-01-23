/*
 * macros.h - Some useful macros
 *
 *
 * zeroskip is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 *
 * Copyright (c) 2018 Partha Susarla <mail@spartha.org>
 *
 */

#ifndef _MACROS_H_
#define _MACROS_H_

#ifdef  __cplusplus
# define CPP_GUARD_START  extern "C" {
# define CPP_GUARD_END    }
#else
# define CPP_GUARD_START
# define CPP_GUARD_END
#endif


#if defined __GNUC__

#define _unused_          __attribute__((unused))
#define _cleanup_(x)      __attribute__((cleanup(x)))
#define _packed_          __attribute__((packed))
#define _public_          __attribute__((visibility("default")))
#define _hidden_          __attribute__((visibility("hidden")))
#define _likely_(x)       (__builtin_expect(!!(x),1))
#define _unlikely_(x)     (__builtin_expect(!!(x),0))

#if __GNUC__ >= 7
#define _fallthrough_     __attribute__((fallthrough))
#else
#define _fallthrough_ /* fall through */
#endif                /* __GNUC__ >= 7 */

#endif  /* __GNUC__ */

#endif  /* _MACROS_H_ */
