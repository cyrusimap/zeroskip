/*
 * crc32c.c - compute CRC32 using the Castagnoli polynomial
 *
 * This implementation is imported as is from cyrus-imapd project
 */

#ifndef CRC32C_H
#define CRC32C_H

#include <libzeroskip/cstring.h>

#include <sys/uio.h>
#include <stdint.h>

extern void crc32c_init(void);

extern uint32_t crc32c(uint32_t crc, const void *buf, size_t len);
extern uint32_t crc32c_map(const char *base, unsigned len);
extern uint32_t crc32c_buf(const cstring *buf);
extern uint32_t crc32c_cstring(const char *buf);
extern uint32_t crc32c_iovec(struct iovec *iov, int iovcnt);

#endif
