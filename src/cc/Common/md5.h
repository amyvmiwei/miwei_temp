/*
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

/** @file
 * md5 digest routines.
 * This file implements the md5 digest algorithm and also has several
 * helper functions like calculating the md5 of a file or a string.
 * The MD5 algorithm was designed by Ron Rivest in 1991
 * (http://www.ietf.org/rfc/rfc1321.txt).
 */

#ifndef _MD5_H
#define _MD5_H

#ifdef __cplusplus
extern "C" {
#endif

/** @addtogroup Common
 *  @{
 */

/** MD5 context structure; this structure is used to store the internal state
 * of the md5 algorithm.
 */
typedef struct {
  unsigned long total[2];     /*!< number of bytes processed  */
  unsigned long state[4];     /*!< intermediate digest state  */
  unsigned char buffer[64];   /*!< data block being processed */
} md5_context;

/** Initialize and setup a MD5 context structure
 *
 * @param ctx MD5 context to be initialized
 */
void md5_starts(md5_context *ctx);

/** Adds data to the MD5 process buffer
 *
 * @param ctx      MD5 context
 * @param input    buffer holding the data
 * @param ilen     length of the input data
 */
void md5_update(md5_context *ctx, const unsigned char *input, int ilen);

/** Retrieve the final MD5 digest
 *
 * @param ctx      MD5 context
 * @param output   MD5 checksum result
 */
void md5_finish(md5_context *ctx, unsigned char output[16]);

/** Convenience function to calculate the MD5 sum of an input buffer
 *
 * @param input    buffer holding the  data
 * @param ilen     length of the input data
 * @param output   MD5 checksum result
 */
void md5_csum(const unsigned char *input, int ilen, unsigned char output[16]);

/** Convenience function to calculate the MD5 sum of a file
 *
 * @param path     input file name
 * @param output   MD5 checksum result
 * @return         0 if successful, or 1 if fopen failed
 */
int md5_file(const char *path, unsigned char output[16]);

/** Calculates a "Hashed MAC" of an input buffer combined with a secret key
 *
 * @param key      HMAC secret key
 * @param keylen   length of the HMAC key
 * @param input    buffer holding the  data
 * @param ilen     length of the input data
 * @param output   HMAC-MD5 result
 */
void md5_hmac(unsigned char *key, int keylen,
              const unsigned char *input, int ilen,
              unsigned char output[16]);

/** Runs a self test.
 *
 * @return         0 if successful, or 1 if the test failed
 */
int md5_self_test(void);

/** Convenience function to calculate the MD5 sum of an input buffer;
 * returns string with the MD5 encoded in hexadecimal 
 *
 * @param input    input string
 * @param ilen     length of the input data
 * @param output   Hex string representation of MD5 checksum
 */
void md5_hex(const void *input, size_t ilen, char output[33]);

/** Calculates the hex string of MD5 of null terminated input
 *
 * @param input    input string
 * @param output   Hex string representation of MD5 checksum
 */
void md5_string(const char *input, char output[33]);

/** Returns a 64-bit hash checksum of a null terminated input buffer
 *
 * @param input    input string
 */
int64_t md5_hash(const char *input);

/** Get the modified base64 encoded string of the first 12 Bytes of the 16 Byte
 * MD5 code of a null terminated output;
 * see http://en.wikipedia.org/wiki/Base64#URL_applications for more
 * information
 *
 * @param input  Pointer to the input string
 * @param output Hex string representation of MD5 checksum
 */
void md5_trunc_modified_base64(const char *input, char output[17]);

/** Get the modified base64 encoded string of a 16 byte message digest
 * see http://en.wikipedia.org/wiki/Base64#URL_applications for more
 * information
 *
 * @param digest The 16 byte input buffer
 * @param output Hex string representation of MD5 checksum
 */
void digest_to_trunc_modified_base64(const char digest[16], char output[17]);

/** @} */

#ifdef __cplusplus
}
#endif

#endif /* md5.h */
