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
 * Implementation of the Lookup3 hash algorithm.
 * The lookup3 hash from Bob Jenkins is a fast and strong 32-bit hash.
 */

#ifndef HYPERTABLE_LOOKUP3_H
#define HYPERTABLE_LOOKUP3_H

#include "Common/String.h"

namespace Hypertable {

/** @addtogroup Common
 *  @{
 */

/** 
 * The Lookup3 hash algorithm, a fast and strong 32bit hash.
 */
class Lookup3 {
  public:
    /** Returns the hash of a String
     *
     * @param s The string to hash
     * @return The 32bit hash value
     */
    uint32_t operator()(const String& s) const {
      return hashlittle(s.c_str(), s.length(), s.length());
    }

    /** Returns the hash of a memory buffer
     *
     * @param start Pointer to the memory buffer to hash
     * @param len Size of the memory buffer, in bytes
     * @return The 32bit hash value
     */
    uint32_t operator()(const void *start, size_t len) const {
      return hashlittle(start, len, len);
    }

    /** Returns the hash of a memory buffer
     *
     * @param start Pointer to the memory buffer to hash
     * @param len Size of the memory buffer, in bytes
     * @param init Initialization value for the hash
     * @return The 32bit hash value
     */
    uint32_t operator()(const void *start, size_t len, uint32_t init) const {
      return hashlittle(start, len, init);
    }

  private:
    /** The lookup3 hash from Bob Jenkins, a fast and strong 32-bit hash */
    static uint32_t hashlittle(const void *data, size_t len, uint32_t hash);
};

/** @} */

} // namespace Hypertable

#endif // HYPERTABLE_LOOKUP3_H
