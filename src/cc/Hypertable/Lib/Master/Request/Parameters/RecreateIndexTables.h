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
/// Declarations for RecreateIndexTables request parameters.
/// This file contains declarations for RecreateIndexTables, a class for encoding and
/// decoding paramters to the <i>recreate index tables</i> %Master operation.

#ifndef Hypertable_Lib_Master_Request_Parameters_RecreateIndexTables_h
#define Hypertable_Lib_Master_Request_Parameters_RecreateIndexTables_h

#include <Hypertable/Lib/TableParts.h>

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

  /// %Request parameters for <i>recreate index tables</i> operation.
  class RecreateIndexTables : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    RecreateIndexTables() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Sets #m_name to
    /// <code>name</code> and #m_parts to <code>parts</code>.
    /// @param name %Table name
    /// @param parts %Table parts
    RecreateIndexTables(const std::string &name, TableParts parts);

    /// Gets name of table to create.
    /// @return Name of table to create
    const string& name() const { return m_name; }

    /// Gets table parts.
    /// @return %Table parts
    const TableParts& parts() const { return m_parts; }

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

    /// %Table name
    std::string m_name;

    /// Which index tables to recreate
    TableParts m_parts;

  };

  /// @}

}}}}}

#endif // Hypertable_Lib_Master_Request_Parameters_RecreateIndexTables_h
