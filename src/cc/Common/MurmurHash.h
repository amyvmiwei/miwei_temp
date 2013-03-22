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
 * MurmurHash2 digest routine.
 * The MurmurHash 2 from Austin Appleby is faster and better mixed (but weaker
 * crypto-wise with one pair of obvious differential) than both Lookup3 and
 * SuperFastHash. Not-endian neutral for speed.
 * https://sites.google.com/site/murmurhash/
 */

#ifndef HYPERTABLE_MURMURHASH_H
#define HYPERTABLE_MURMURHASH_H

#include "Common/String.h"

namespace Hypertable {

/** @addtogroup Common
 *  @{
 */

/**
 * The murmurhash2 implementation
 *
 * @param data Pointer to the input buffer
 * @param len Size of the input buffer
 * @param hash Initial seed for the hash; usually set to 0
 * @return The 32bit hash of the input buffer
 */
extern uint32_t murmurhash2(const void *data, size_t len, uint32_t hash);

/**
 * Helper structure using overloaded operator() to calculate hashes of various
 * input types.
 *
 * This class is usually used as a template parameter, i.e. see
 * BlobHashTraits.h.
 */
struct MurmurHash2 {
  /** Returns hash of a String */
  uint32_t operator()(const String& s) const {
    return murmurhash2(s.c_str(), s.length(), 0);
  }

  /** Returns hash of a memory buffer */
  uint32_t operator()(const void *start, size_t len, uint32_t seed = 0) const {
    return murmurhash2(start, len, seed);
  }

  /** Returns hash of a null terminated memory buffer */
  uint32_t operator()(const char *s) const {
    return murmurhash2(s, strlen(s), 0);
  }
};

/** @} */

} // namespace Hypertable

#endif // HYPERTABLE_MURMURHASH_H
