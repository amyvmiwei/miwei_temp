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
/// Declarations for AlterTable request parameters.
/// This file contains declarations for AlterTable, a class for encoding and
/// decoding paramters to the <i>alter table</i> %Master operation.

#ifndef Hypertable_Lib_Master_Request_Parameters_AlterTable_h
#define Hypertable_Lib_Master_Request_Parameters_AlterTable_h

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

  /// %Request parameters for <i>alter table</i> operation.
  class AlterTable : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    AlterTable() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Sets #m_name to
    /// <code>name</code> and #m_schema to <code>schema</code>.
    /// @param name Table name
    /// @param schema Table schema
    /// @param force Force schema alteration even if generation does not match
    /// original
    AlterTable(const std::string &name, const std::string &schema, bool force);

    /// Gets name of table to alter.
    /// @return Name of table to alter
    const string& name() const { return m_name; }

    /// Gets table schema.
    /// @return Table schema
    const string& schema() const { return m_schema; }

    /// Gets force flag.
    /// @return Force flag
    bool force() { return m_force; }

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

    /// Table name
    std::string m_name;

    /// New schema
    std::string m_schema;

    /// Force schema alteration even if generation does not match original
    bool m_force {};

  };

  /// @}

}}}}}

#endif // Hypertable_Lib_Master_Request_Parameters_AlterTable_h
