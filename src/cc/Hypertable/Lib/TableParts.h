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

/// @file
/// Declarations for TableParts.
/// This file contains declarations for TableParts, a class that represents a
/// set of table parts (i.e. PRIMARY, VALUE_INDEX, QUALIFIER_INDEX)

#ifndef HYPERTABLE_TABLEPARTS_H
#define HYPERTABLE_TABLEPARTS_H

#include <Common/Serializable.h>

#include <string>

namespace Hypertable {

  /// @addtogroup libHypertable
  /// @{

  /// Represents a set of table parts (sub-tables).
  /// A table consists of a PRIMARY table and optionally, an associated
  /// VALUE_INDEX table and an associated QUALIFIER_INDEX table.  An object of
  /// this class can be used to hold information about the portions of a table
  /// to which an operation applies.  It provides member functions for testing
  /// whether or not a table part is included in the set, a member function for
  /// rendering the set as a human-readable string, and a serialization
  /// interface.
  class TableParts : public Serializable {

  public:

    /// Enumeration for describing parts of a table
    enum {
      /// Primary table
      PRIMARY = 0x01,
      /// Value index
      VALUE_INDEX = 0x02,
      /// %Qualifier index
      QUALIFIER_INDEX = 0x04,
      /// All parts
      ALL = 0x07
    };

    /// Constructor.
    /// @param parts Bitmask describing table parts
    TableParts(int8_t parts=0) : m_parts(parts) { }

    /// Test if primary table is included in set.
    /// @return <i>true</i> if primary table is included in set, <i>false</i>
    /// otherwise
    inline bool primary() const { return m_parts & PRIMARY; }

    /// Test if value index is included in set.
    /// @return <i>true</i> if value index is included in set, <i>false</i>
    /// otherwise
    inline bool value_index() const { return m_parts & VALUE_INDEX; }

    /// Test if qualifier index is included in set.
    /// @return <i>true</i> if qualifier index is included in set, <i>false</i>
    /// otherwise
    inline bool qualifier_index() const { return m_parts & QUALIFIER_INDEX; }

    /// Returns serialized length
    /// @return Serialized length
    /// @see encode() for serialization format
    virtual size_t encoded_length() const override { return 1; };

    /// Writes serialized representation to a buffer.
    /// Serialized format is as follows:
    /// <table>
    ///   <tr>
    ///   <th>Encoding</th><th>Description</th>
    ///   </tr>
    ///   <tr>
    ///   <td>1 byte</td><td>Bitmask of part bits</td>
    ///   </tr>
    /// </table>
    /// @param bufp Address of destination buffer pointer (advanced by call)
    virtual void encode(uint8_t **bufp) const override;

    /// Reads serialized representation from a buffer.
    /// @param bufp Address of destination buffer pointer (advanced by call)
    /// @param remainp Address of integer holding amount of remaining buffer
    /// @see encode() for serialization format
    virtual void decode(const uint8_t **bufp, size_t *remainp) override;

    /// Returns human readable string describing table parts.
    /// @return Human readable string describing table parts.
    const std::string to_string() const;

    /// Checks if any table parts are specified.
    /// @return <i>true</i> if any table parts are specified, <i>false</i>
    /// otherwise.
    operator bool () const { return m_parts != 0; }

    /// Clears all parts.
    void clear() { m_parts = 0; }

  private:

    /// Bitmask representing table parts.
    int8_t m_parts {};
  };

  /// @}
}


#endif // HYPERTABLE_TABLEPARTS_H
