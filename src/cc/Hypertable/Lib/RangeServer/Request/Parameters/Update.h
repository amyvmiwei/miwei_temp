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
/// Declarations for Update request parameters.
/// This file contains declarations for Update, a class for encoding and
/// decoding paramters to the <i>update</i> %RangeServer function.

#ifndef Hypertable_Lib_RangeServer_Request_Parameters_Update_h
#define Hypertable_Lib_RangeServer_Request_Parameters_Update_h

#include <Hypertable/Lib/RangeSpec.h>
#include <Hypertable/Lib/RangeState.h>
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

  /// %Request parameters for <i>update</i> function.
  class Update : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    Update() {}

    /// Constructor.
    /// Initializes with parameters for encoding.
    /// @param cluster_id Cluster ID
    /// @param table %Table identifier
    /// @param count %Update count
    /// @param flags Flags
    Update(uint64_t cluster_id, const TableIdentifier &table, int32_t count,
           int32_t flags) : m_cluster_id(cluster_id), m_table(table),
                            m_count(count), m_flags(flags) {}

    /// Gets cluster ID
    /// @return Cluster ID
    uint64_t cluster_id() { return m_cluster_id; }

    /// Gets table identifier
    /// @return %Table identifier
    const TableIdentifier &table() { return m_table; }

    /// Gets update count
    /// @return %Update count
    int32_t count() { return m_count; }

    /// Gets flags
    /// @return Flags
    int32_t flags() { return m_flags; }

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

    /// Cluster ID
    uint64_t m_cluster_id {};

    /// %Table identifier
    TableIdentifier m_table;

    /// %Update count
    int32_t m_count {};

    /// Flags
    int32_t m_flags {};
  };

  /// @}

}}}}}

#endif // Hypertable_Lib_RangeServer_Request_Parameters_Update_h
