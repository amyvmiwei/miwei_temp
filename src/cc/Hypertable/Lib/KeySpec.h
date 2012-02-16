/** -*- c++ -*-
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

#ifndef HYPERTABLE_KEYSPEC_H
#define HYPERTABLE_KEYSPEC_H

#include <boost/noncopyable.hpp>

#include <vector>

#include "Common/String.h"
#include "Common/Error.h"

namespace Hypertable {

  static const int64_t TIMESTAMP_MIN  = INT64_MIN;
  static const int64_t TIMESTAMP_MAX  = INT64_MAX;
  static const int64_t TIMESTAMP_NULL = INT64_MIN + 1;
  static const int64_t TIMESTAMP_AUTO = INT64_MIN + 2;
  static const int64_t AUTO_ASSIGN    = INT64_MIN + 2;

  static const uint32_t FLAG_DELETE_ROW            = 0x00;
  static const uint32_t FLAG_DELETE_COLUMN_FAMILY  = 0x01;
  static const uint32_t FLAG_DELETE_CELL           = 0x02;
  static const uint32_t FLAG_DELETE_CELL_VERSION   = 0x03;
  enum {
    KEYSPEC_DELETE_MAX = 4
  };
  static const uint32_t FLAG_INSERT                = 0xFF;

  class KeySpec {
  public:

    KeySpec() : row(0), row_len(0), column_family(0), column_qualifier(0),
                column_qualifier_len(0), timestamp(AUTO_ASSIGN),
                revision(AUTO_ASSIGN), flag(FLAG_INSERT) {}

    explicit KeySpec(const char *r, const char *cf, const char *cq,
                     int64_t ts = AUTO_ASSIGN, uint8_t flag_=FLAG_INSERT)
      : row(r), row_len(r ? strlen(r) : 0), column_family(cf),
        column_qualifier(cq), column_qualifier_len(cq ? strlen(cq) : 0),
        timestamp(ts), revision(AUTO_ASSIGN), flag(flag_) {}

    explicit KeySpec(const char *r, const char *cf,
                     int64_t ts = AUTO_ASSIGN, uint8_t flag_=FLAG_INSERT)
      : row(r), row_len(r ? strlen(r) : 0), column_family(cf),
        column_qualifier(0), column_qualifier_len(0),
        timestamp(ts), revision(AUTO_ASSIGN), flag(flag_) {}

    void clear() {
      row = 0;
      row_len = 0;
      column_family = 0;
      column_qualifier = 0;
      column_qualifier_len = 0;
      timestamp = AUTO_ASSIGN;
      revision = AUTO_ASSIGN;
      flag = FLAG_INSERT;
    }

    void sanity_check() const {
      const char *r = (const char *)row;
      const char *cq = (const char *)column_qualifier;

      // Sanity check the row key
      if (row_len == 0)
        HT_THROW(Error::BAD_KEY, "Invalid row key - cannot be zero length");

      if (r[row_len] != 0)
        HT_THROW(Error::BAD_KEY,
                 "Invalid row key - must be followed by a '\\0' character");

      if (strlen(r) != row_len)
        HT_THROWF(Error::BAD_KEY, "Invalid row key - '\\0' character not "
                 "allowed (offset=%d)", (int)strlen(r));

      if (r[0] == (char)0xff && r[1] == (char)0xff)
        HT_THROW(Error::BAD_KEY, "Invalid row key - cannot start with "
                 "character sequence 0xff 0xff");

      // Sanity check the column qualifier
      if (column_qualifier_len > 0) {
        if (cq[column_qualifier_len] != 0)
          HT_THROW(Error::BAD_KEY, "Invalid column qualifier - must be followed"
                   " by a '\\0' character");
        if (strlen(cq) != column_qualifier_len)
          HT_THROWF(Error::BAD_KEY, "Invalid column qualifier - '\\0' character"
                    " not allowed (offset=%d)", (int)strlen(cq));
      }

      if (flag > FLAG_DELETE_ROW && flag < FLAG_INSERT) {
        if (column_family == 0)
          HT_THROWF(Error::BAD_KEY, "Flag is set to %d but column family is null", flag);
        if (flag > FLAG_DELETE_COLUMN_FAMILY && flag < FLAG_INSERT) {
          if (column_qualifier == 0)
            HT_THROWF(Error::BAD_KEY, "Flag is set to %d but column qualifier is null", flag);
          if (flag == FLAG_DELETE_CELL_VERSION && timestamp == AUTO_ASSIGN)
            HT_THROWF(Error::BAD_KEY, "Flag is set to %d but timestamp is AUTO_ASSIGN", flag);
        }
      }
    }

    const void  *row;
    size_t       row_len;
    const char  *column_family;
    const char  *column_qualifier;
    size_t       column_qualifier_len;
    int64_t      timestamp;
    int64_t      revision;  // internal use only
    uint8_t      flag;
  };

  std::ostream &operator<<(std::ostream &, const KeySpec &);


  /**
   * Helper class for building a KeySpec.  This class manages the allocation
   * of all string members.
   */
  class KeySpecBuilder : boost::noncopyable {
  public:
    void set_row(const String &row) {
      m_strings.push_back(row);
      m_key_spec.row = m_strings.back().c_str();
      m_key_spec.row_len = m_strings.back().length();
    }

    void set_column_family(const String &cf) {
      m_strings.push_back(cf);
      m_key_spec.column_family = m_strings.back().c_str();
    }

    void set_column_qualifier(const String &cq) {
      m_strings.push_back(cq);
      m_key_spec.column_qualifier = m_strings.back().c_str();
      m_key_spec.column_qualifier_len = m_strings.back().length();
    }

    void set_timestamp(int64_t timestamp) {
      m_key_spec.timestamp = timestamp;
    }

    void set_flag(uint8_t flag_) {
      m_key_spec.flag = flag_;
    }


    /**
     * Clears the state.
     */
    void clear() {
      m_key_spec.clear();
      m_strings.clear();
    }

    /**
     * Returns the built KeySpec object
     *
     * @return reference to built KeySpec object
     */
    KeySpec &get() { return m_key_spec; }

  private:
    std::vector<String> m_strings;
    KeySpec m_key_spec;
  };


} // namespace Hypertable

#endif // HYPERTABLE_KEYSPEC_H

