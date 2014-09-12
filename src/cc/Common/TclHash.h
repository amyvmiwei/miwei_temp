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
 * Implementations of the Tcl hash algorithm by John Ousterhout.
 */

#ifndef HYPERTABLE_TCLHASH_H
#define HYPERTABLE_TCLHASH_H

#include "Common/String.h"

namespace Hypertable {

/** @addtogroup Common
 *  @{
 */

/**
 * The Tcl hash by John Ousterhout for null-terminated c-strings, preferably
 * alpha-numeric characters only.
 *
 * @param s Pointer to the null-terminated c-string 
 * @return The calculated hash value
 */
inline size_t tcl_hash(const char *s) {
  size_t ret = 0;

  for (; *s; ++s)
    ret += (ret << 3) + (unsigned)*s;

  return ret;
}

/**
 * Tcl hash for data with known length
 *
 * @param data A pointer to the memory buffer
 * @param len The length of the buffer, in bytes
 * @param seed An initial seed
 * @return The calculated hash value
 */
inline size_t tcl_hash(const void *data, size_t len, size_t seed) {
  size_t ret = seed;
  const uint8_t *dp = (uint8_t *)data, *end = dp + len;

  for (; dp < end; ++dp)
    ret += (ret << 3) + *dp;

  return ret;
}

#define HT_TCLHASH_DO2(p, i) \
  ret += (ret << 3) + p[i]; ret += (ret << 3) + p[i+1]

#define HT_TCLHASH_DO4(p, i) HT_TCLHASH_DO2(p, i); HT_TCLHASH_DO2(p, i+2);
#define HT_TCLHASH_DO8(p, i) HT_TCLHASH_DO4(p, i); HT_TCLHASH_DO4(p, i+4);

/**
 * Unrolled Tcl hash, up to 20% faster for longer (> 8 bytes) strings
 *
 * @param data A pointer to the memory buffer
 * @param len The length of the buffer, in bytes
 * @param seed An initial seed
 * @return The calculated hash value
 */
inline size_t tcl_hash2(const void *data, size_t len, size_t seed) {
  size_t ret = seed;
  const uint8_t *dp = (uint8_t *)data;

  if (len >= 8) do {
    HT_TCLHASH_DO8(dp, 0);
    dp += 8;
    len -= 8;
  } while (len >= 8);

  if (len > 0) do {
    ret += (ret << 3) + *dp++;
  } while (--len);

  return ret;
}

/**
 * Helper structure using overloaded operator() to calculate the Tcl hashes
 * of various input types.
 *
 * This class is usually used as a template parameter, i.e. see
 * BlobHashTraits.h.
 */
struct TclHash {
  /** Returns hash of a memory buffer with an optional seed */
  size_t operator()(const void *start, size_t len, size_t seed = 0) const {
    return tcl_hash(start, len, seed);
  }

  /** Returns hash of a String */
  size_t operator()(const String& s) const {
    return tcl_hash(s.c_str(), s.length(), 0);
  }

  /** Returns hash of a null-terminated c-String */
  size_t operator()(const char *s) const { return tcl_hash(s); }
};

/**
 * Helper structure using overloaded operator() to calculate the Tcl hashes
 * of various input types (using %tcl_hash2, the faster implementation).
 *
 * This class is usually used as a template parameter, i.e. see
 * BlobHashTraits.h.
 */
struct TclHash2 {
  /** Returns hash of a memory buffer with an optional seed */
  size_t operator()(const void *start, size_t len, size_t seed = 0) const {
    return tcl_hash2(start, len, seed);
  }

  /** Returns hash of a String */
  size_t operator()(const String& s) const {
    return tcl_hash2(s.c_str(), s.length(), 0);
  }

  /** Returns hash of a null-terminated c-String */
  size_t operator()(const char *s) const {
    return tcl_hash2(s, strlen(s), 0);
  }
};

/** @} */

} // namespace Hypertable

#endif // HYPERTABLE_TCLHASH_H
