/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
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

#ifndef Hypertable_Lib_RowInterval_h
#define Hypertable_Lib_RowInterval_h

#include <Common/Serializable.h>

#include <ostream>

namespace Hypertable {
namespace Lib {

  using namespace std;

  /**
   * Represents a row interval.  c-string data members are not managed
   * so caller must handle (de)allocation.
   */
  class RowInterval : public Serializable {
  public:
    RowInterval() { }
    RowInterval(const char *start_row, bool start_row_inclusive,
                const char *end_row, bool end_row_inclusive)
      : start(start_row), start_inclusive(start_row_inclusive),
        end(end_row), end_inclusive(end_row_inclusive) { }
    RowInterval(const uint8_t **bufp, size_t *remainp) {
      decode(bufp, remainp);
    }

    virtual ~RowInterval() {}

    /// Renders row interval as HQL.
    /// @return HQL string representing row interval.
    const string render_hql() const;

    const char *start {};
    bool start_inclusive {true};
    const char *end {};
    bool end_inclusive {true};

  private:

    /// Returns encoding version.
    /// @return Encoding version
    uint8_t encoding_version() const override;

    /// Returns internal serialized length.
    /// @return Internal serialized length
    /// @see encode_internal() for encoding format
    size_t encoded_length_internal() const override;

    /// Writes serialized representation of object to a buffer.
    /// @param bufp Address of destination buffer pointer (advanced by call)
    void encode_internal(uint8_t **bufp) const override;

    /// Reads serialized representation of object from a buffer.
    /// @param version Encoding version
    /// @param bufp Address of destination buffer pointer (advanced by call)
    /// @param remainp Address of integer holding amount of serialized object
    /// remaining
    /// @see encode_internal() for encoding format
    void decode_internal(uint8_t version, const uint8_t **bufp,
			 size_t *remainp) override;

  };

  ostream &operator<<(ostream &os, const RowInterval &ri);

}}

#endif // Hypertable_Lib_RowInterval_h
