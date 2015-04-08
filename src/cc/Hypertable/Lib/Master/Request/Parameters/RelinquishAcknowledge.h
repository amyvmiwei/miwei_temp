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
/// Declarations for RelinquishAcknowledge request parameters.
/// This file contains declarations for RelinquishAcknowledge, a class for encoding and
/// decoding paramters to the <i>relinquish acknowledge</i> %Master operation.

#ifndef Hypertable_Lib_Master_Request_Parameters_RelinquishAcknowledge_h
#define Hypertable_Lib_Master_Request_Parameters_RelinquishAcknowledge_h

#include <Common/Serializable.h>

#include <Hypertable/Lib/RangeSpec.h>
#include <Hypertable/Lib/TableIdentifier.h>

#include <string>

using namespace std;

namespace Hypertable {
namespace Lib {
namespace Master {
namespace Request {
namespace Parameters {

  /// @addtogroup libHypertableMasterRequestParameters
  /// @{

  /// %Request parameters for <i>relinquish acknowledge</i> operation.
  class RelinquishAcknowledge : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    RelinquishAcknowledge() {}

    /// Constructor.
    /// Initializes with parameters for encoding.
    /// @param source %RangeServer from which range was being moved
    /// @param range_id %Range MetaLog entry identifier
    /// @param table %Table identifier of table to which range belongs
    /// @param range_spec %Range specification
    RelinquishAcknowledge(const string &source, int64_t range_id,
                          const TableIdentifier &table,
                          const RangeSpec &range_spec)
      : m_source(source), m_range_id(range_id), m_table(table),
        m_range_spec(range_spec) { }

    /// Gets name of source %RangeServer.
    /// @return Name of source %RangeServer.
    const string& source() const { return m_source; }

    /// Gets range MetaLog entry identifier
    /// @return Range MetaLog entry identifier
    int64_t range_id() const { return m_range_id; }

    /// Gets table identifier.
    /// @return Reference to table identifier.
    TableIdentifier &table() { return m_table; }

    /// Gets range specification.
    /// @return Reference to range specification.
    RangeSpec &range_spec() { return m_range_spec; }

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

    /// Source %RangeServer
    string m_source;

    /// %Range MetaLog entry identifier
    int64_t m_range_id;

    /// %Table identifier of table to which range belongs
    TableIdentifierManaged m_table;

    /// %Range specification
    RangeSpecManaged m_range_spec;

  };

  /// @}

}}}}}

#endif // Hypertable_Lib_Master_Request_Parameters_RelinquishAcknowledge_h
