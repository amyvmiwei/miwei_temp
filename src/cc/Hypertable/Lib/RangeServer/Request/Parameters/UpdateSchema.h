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
/// Declarations for UpdateSchema request parameters.
/// This file contains declarations for UpdateSchema, a class for encoding and
/// decoding paramters to the <i>update schema</i> %RangeServer function.

#ifndef Hypertable_Lib_RangeServer_Request_Parameters_UpdateSchema_h
#define Hypertable_Lib_RangeServer_Request_Parameters_UpdateSchema_h

#include <Hypertable/Lib/TableIdentifier.h>

#include <Common/Serializable.h>

#include <string>

using namespace std;

namespace Hypertable {
namespace Lib {
namespace RangeServer {
namespace Request {
namespace Parameters {

  /// @addtogroup libHypertableRangeServerRequestParameters
  /// @{

  /// %Request parameters for <i>update schema</i> function.
  class UpdateSchema : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    UpdateSchema() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Initializes #m_table with
    /// <code>table</code> and #m_schema with <code>schema</code>.
    /// @param table %Table identifier
    /// @param schema %Table schema
    UpdateSchema(const TableIdentifier &table, const string &schema)
      : m_table(table), m_schema(schema.c_str()) { }

    /// Gets table identifier
    /// @return %Table identifier
    const TableIdentifier &table() { return m_table; }

    /// Gets table schema
    /// @return %Table schema
    const char *schema() { return m_schema; }

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

    /// %Table identifier
    TableIdentifier m_table;

    /// %Table schema
    const char *m_schema;

  };

  /// @}

}}}}}

#endif // Hypertable_Lib_RangeServer_Request_Parameters_UpdateSchema_h
