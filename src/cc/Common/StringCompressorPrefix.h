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
 * A class to prefix-compress strings. See %StringDecompressorPrefix for the
 * counterpart.
 */

#ifndef HYPERTABLE_STRINGCOMPRESSORPREFIX_H
#define HYPERTABLE_STRINGCOMPRESSORPREFIX_H

#include "ReferenceCount.h"
#include "DynamicBuffer.h"
#include "String.h"
#include "Serialization.h"

namespace Hypertable {

  /** @addtogroup Common
   *  @{
   */

  /**
   * A class to prefix-compress strings.
   */
  class StringCompressorPrefix: public ReferenceCount {
  public:
    /** Clears the internal state */
    virtual void reset() {
      m_last_string.clear();
      m_compressed_string.clear();
    }

    /** Adds (and compresses) a string. Keeps a copy of the uncompressed string
     * for the next invocation of this function. The compressed string can be
     * retrieved with %write.
     *
     * @param str A null-terminated string to compress
     */
    virtual void add(const char *str) {
      m_uncompressed_len = strlen(str);
      size_t prefix_len = get_prefix_length(str, m_uncompressed_len);
      size_t suffix_len;
      // null terminate suffix
      if (prefix_len > m_uncompressed_len)
        suffix_len = 1;
      else
        suffix_len = m_uncompressed_len  - prefix_len + 1;

      m_compressed_len = Serialization::encoded_length_vi32(prefix_len)
          + suffix_len;

      m_compressed_string.clear();
      m_compressed_string.reserve(m_compressed_len);
      Serialization::encode_vi32(&m_compressed_string.ptr, prefix_len);
      memcpy(m_compressed_string.ptr, (const void*) (str + prefix_len),
              suffix_len);
      m_last_string = str;
    }

    /** Retrieves the length of the compressed string */
    virtual size_t length() const {
      return m_compressed_len;
    }

    /** Retrieves the length of the uncompressed string */
    virtual size_t length_uncompressed() const {
      return m_uncompressed_len;
    }

    /** Writes the compressed string to a buffer */
    virtual void write(uint8_t *buf) const {
      memcpy(buf, (const void *)m_compressed_string.base, m_compressed_len);
    }

    /** Writes the uncompressed string to a buffer */
    virtual void write_uncompressed(uint8_t *buf) const {
      // null terminated string
      memcpy(buf, (const void *)m_last_string.c_str(),
              m_last_string.length() + 1);
    }

  private:
    /** Helper function returning the prefix length of a new string */
    size_t get_prefix_length(const char *str, size_t len) const {
      len = len > m_last_string.length() ? m_last_string.length() : len;
      size_t ii;

      for (ii = 0; ii < len; ++ii)
        if (m_last_string[ii] != str[ii])
          break;
      return ii;
    }

    /** Length of the compressed string */
    size_t m_compressed_len;

    /** Length of the uncompressed string */
    size_t m_uncompressed_len;

    /** The previously added (uncompressed) string */
    String m_last_string;

    /** Dynamic array holding the compressed string */
    DynamicBuffer m_compressed_string;
  };

  typedef intrusive_ptr<StringCompressorPrefix> StringCompressorPrefixPtr;

  /** @} */
}

#endif // HYPERTABLE_STRINGCOMPRESSORPREFIX_H
