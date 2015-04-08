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
/// Declarations for DropTable request parameters.
/// This file contains declarations for DropTable, a class for encoding and
/// decoding paramters to the <i>drop table</i> %Master operation.

#ifndef Hypertable_Lib_Master_Request_Parameters_DropTable_h
#define Hypertable_Lib_Master_Request_Parameters_DropTable_h

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

  /// %Request parameters for <i>drop table</i> operation.
  class DropTable : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    DropTable() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Sets #m_name to
    /// <code>name</code> and #m_if_exists to <code>if_exists</code>.
    /// @param name Table name
    /// @param if_exists Flag indicating operation should succeed if table does
    /// not exist
    DropTable(const std::string &name, bool if_exists);

    /// Gets name of table to drop.
    /// @return Name of table to drop
    const string& name() const { return m_name; }

    /// Gets <i>if exists</i> flag
    /// @return <i>if exists</i> flag
    bool if_exists() { return m_if_exists; }

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

    /// Flag indicating operation should succeed if table does not exist
    bool m_if_exists {};

  };

  /// @}

}}}}}

#endif // Hypertable_Lib_Master_Request_Parameters_DropTable_h
