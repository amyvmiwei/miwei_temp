/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
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

/// @file
/// Declarations for RenameTable request parameters.
/// This file contains declarations for RenameTable, a class for encoding and
/// decoding paramters to the <i>rename table</i> %Master operation.

#ifndef Hypertable_Lib_Master_Request_Parameters_RenameTable_h
#define Hypertable_Lib_Master_Request_Parameters_RenameTable_h

#include <Common/Serializable.h>

#include <string>

using namespace std;

namespace Hypertable {
namespace Lib {
namespace Master {
namespace Request {
namespace Parameters {

  /// @addtogroup libHypertableMasterRequestParameters
  /// @{

  /// %Request parameters for <i>rename table</i> operation.
  class RenameTable : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    RenameTable() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Sets #m_from to
    /// <code>from</code> and #m_to to <code>to</code>.
    /// @param from Original table name
    /// @param to New table name
    RenameTable(const std::string &from, const std::string &to);

    /// Gets original table name.
    /// @return Original table name.
    const string& from() const { return m_from; }

    /// Gets new table name.
    /// @return New table name
    const string& to() const { return m_to; }

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

    /// Original table name
    std::string m_from;

    /// New table name
    std::string m_to;

  };

  /// @}

}}}}}

#endif // Hypertable_Lib_Master_Request_Parameters_RenameTable_h
