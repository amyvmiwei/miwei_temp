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
 * SuperFastHash hash algorithm.
 * The SuperFastHash from Paul Hsieh, recommended by google sparse hash
 * folks. Slightly faster but weaker (crypto-wise) than Lookup3
 * see http://www.azillionmonkeys.com/qed/weblicense.html
 */

#ifndef HYPERTABLE_SUPERFASTHASH_H
#define HYPERTABLE_SUPERFASTHASH_H

#include "Common/String.h"

namespace Hypertable {

/** @addtogroup Common
 *  @{
 */

/**
 * The SuperFastHash implementation
 *
 * @param data Pointer to the input buffer
 * @param len Size of the input buffer
 * @param hash Initial seed for the hash; usually set to 0
 * @return The 32bit hash of the input buffer
 */
extern uint32_t superfasthash(const void *data, size_t len, uint32_t hash);

/**
 * Helper structure using overloaded operator() to calculate hashes of various
 * input types.
 *
 * This class is usually used as a template parameter, i.e. see
 * BlobHashTraits.h.
 */
struct SuperFastHash {
  /** Returns the hash of a string */
  uint32_t operator()(const String &s) const {
    return superfasthash(s.c_str(), s.length(), s.length());
  }

  /** Returns the hash of a memory buffer */
  uint32_t operator()(const void *start, size_t len) const {
    return superfasthash(start, len, len);
  }

  /** Returns the hash of a memory buffer; sets an initial seed */
  uint32_t operator()(const void *start, size_t len, uint32_t seed) const {
    return superfasthash(start, len, seed);
  }

  /** Returns the hash of a null terminated memory buffer */
  uint32_t operator()(const char *s) const {
    size_t len = strlen(s);
    return superfasthash(s, len, len);
  }
};

/** @} */

} // namespace Hypertable

#endif // HYPERTABLE_SUPERFASTHASH_H
