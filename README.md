[![Build Status:master](https://api.travis-ci.org/ajaysusarla/zeroskip.svg?branch=master)](https://travis-ci.org/ajaysusarla/zeroskip)

# Zeroskip

Zeroskip is a key-value database format. This repository provides
the library to create and use Zeroskip databases.

## Build and Install
### Dependencies
Zeroskip needs `libz`(zlib) for crc32 functions and `libuuid` for
uuid_* functions. The build system uses `pkg-config` to determine the
`libs` and `include` paths. So please ensure that the the
`PKG_CONFIG_PATH` contains the right path. 

On `Solaris`(`Triton`) it is: `PKG_CONFIG_PATH=/opt/local/lib/pkgconfig`.

