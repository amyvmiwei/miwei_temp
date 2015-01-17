/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

#ifndef Hypertable_Lib_CellInterval_h
#define Hypertable_Lib_CellInterval_h

#include <Common/Serializable.h>

#include <ostream>

namespace Hypertable {
namespace Lib {

  using namespace std;

  /**
   * Represents a cell interval.  c-string data members are not managed
   * so caller must handle (de)allocation.
   */
  class CellInterval : public Serializable {
  public:
    CellInterval() { }
    CellInterval(const char *start_row, const char *start_column,
                 bool start_inclusive, const char *end_row,
                 const char *end_column, bool end_inclusive)
      : start_row(start_row), start_column(start_column),
        start_inclusive(start_inclusive), end_row(end_row),
        end_column(end_column), end_inclusive(end_inclusive) { }
    CellInterval(const uint8_t **bufp, size_t *remainp) {
      decode(bufp, remainp);
    }

    virtual ~CellInterval() {}

    /// Renders cell interval as HQL.
    /// @return HQL string representing cell interval.
    const string render_hql() const;

    const char *start_row {};
    const char *start_column {};
    bool start_inclusive {true};
    const char *end_row {};
    const char *end_column {};
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

  ostream &operator<<(ostream &os, const CellInterval &ci);

}}

#endif // Hypertable_Lib_CellInterval_h
