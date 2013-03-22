/*
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
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
 * A class to decompress prefix-compressed strings. See %StringCompressorPrefix
 * for the counterpart.
 */

#ifndef HYPERTABLE_STRINGDECOMPRESSORPREFIX_H
#define HYPERTABLE_STRINGDECOMPRESSORPREFIX_H

#include "ReferenceCount.h"
#include "String.h"

namespace Hypertable {

  /** @addtogroup Common
   *  @{
   */

  /**
   * A class to decompress prefix-compressed strings.
   */
  class StringDecompressorPrefix : public ReferenceCount {
  public:
    /** Constructor; creates a new decompressor */
    StringDecompressorPrefix()
      : m_compressed_len(0) {
    }

    /** Resets and clears the internal state */
    virtual void reset() {
      m_last_string.clear();
      m_compressed_len = 0;
    }

    /** Adds (and decompresses) a compressed string. The string must have been
     * compressed with %StringCompressorPrefix.
     *
     * @param next_base Pointer to the compressed string
     * @return An advanced pointer to the next string
     */
    virtual const uint8_t *add(const uint8_t *next_base) {
      String str;
      const uint8_t *next_ptr = next_base;
      size_t prefix_len = Serialization::decode_vi32(&next_ptr);
      str = m_last_string.substr(0, prefix_len);
      size_t suffix_len = strlen((const char *)next_ptr);
      str.append((const char *)next_ptr);
      m_last_string = str;
      const uint8_t *next = next_ptr + suffix_len + 1;
      m_compressed_len = (size_t)(next - next_base);
      return (next_ptr + suffix_len + 1);
    }

    /** Returns the length of the compressed string */
    virtual size_t length() const {
      return m_compressed_len;
    }

    /** Returns the length of the uncompressed string */
    virtual size_t length_uncompressed() const {
      return m_last_string.size();
    }

    /** Returns the uncompressed string */
    virtual void load(String &str) const {
      str = m_last_string;
    }

  private:
    /** The uncompressed string; use %load to retrieve it */
    String m_last_string;

    /** Length of the compressed string */
    size_t m_compressed_len;
  };

  typedef intrusive_ptr<StringDecompressorPrefix> StringDecompressorPrefixPtr;

  /** @} */

} // namespace Hypertable

#endif // HYPERTABLE_STRINGDECOMPRESSORPREFIX_H
