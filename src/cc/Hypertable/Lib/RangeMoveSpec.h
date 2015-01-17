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

/// @file
/// Declarations for RangeMoveSpec.
/// This file contains declarations for RangeMoveSpec, a class representing the
/// specification of a range move.

#ifndef Hypertable_Lib_RangeMoveSpec_h
#define Hypertable_Lib_RangeMoveSpec_h

#include <Hypertable/Lib/RangeSpec.h>
#include <Hypertable/Lib/TableIdentifier.h>

#include <Common/Serializable.h>

#include <memory>

namespace Hypertable {

  /// @addtogroup libHypertable
  /// @{

  /// Range move specification.
  class RangeMoveSpec : public Serializable {
  public:

    RangeMoveSpec() { clear(); }
    RangeMoveSpec(const char *src, const char *dest, const char* table_id,
                  const char *start_row, const char *end_row) {
      clear();
      source_location = src;
      dest_location = dest;
      table.set_id(table_id);
      range.set_start_row(start_row);
      range.set_end_row(end_row);
    }

    void clear() {
      table.id = 0;
      table.generation = 0;
      range.start_row = 0;
      range.end_row = 0;
      source_location.clear();
      dest_location.clear();
      error = 0;
      complete = false;
    }

    TableIdentifierManaged table;
    RangeSpecManaged range;
    String source_location;
    String dest_location;
    int32_t error {};
    bool complete {};

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

  typedef std::shared_ptr<RangeMoveSpec> RangeMoveSpecPtr;

  std::ostream &operator<<(std::ostream &os, const RangeMoveSpec &move_spec);

  /// @}

}

#endif // Hypertable_Lib_RangeMoveSpec_h
