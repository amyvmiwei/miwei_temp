/**
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

#ifndef HYPERTABLE_SUPERFASTHASH_H
#define HYPERTABLE_SUPERFASTHASH_H

#include "Common/String.h"

namespace Hypertable {

/**
 * The SuperFastHash from Paul Hsieh, recommended by google sparse hash
 * folks. Slightly faster but weaker (crypto-wise) than Lookup3
 */
uint32_t superfasthash(const void *data, size_t len, uint32_t hash);

struct SuperFastHash {
  uint32_t operator()(const String& s) const {
    return superfasthash(s.c_str(), s.length(), s.length());
  }

  uint32_t operator()(const void *start, size_t len) const {
    return superfasthash(start, len, len);
  }

  uint32_t operator()(const void *start, size_t len, uint32_t seed) const {
    return superfasthash(start, len, seed);
  }

  uint32_t operator()(const char *s) const {
    size_t len = strlen(s);
    return superfasthash(s, len, len);
  }
};

} // namespace Hypertable

#endif // HYPERTABLE_SUPERFASTHASH_H
