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
/// Declarations for LoadRange request parameters.
/// This file contains declarations for LoadRange, a class for encoding and
/// decoding paramters to the <i>load range</i> %RangeServer function.

#ifndef Hypertable_Lib_RangeServer_Request_Parameters_LoadRange_h
#define Hypertable_Lib_RangeServer_Request_Parameters_LoadRange_h

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

  /// %Request parameters for <i>load range</i> function.
  class LoadRange : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    LoadRange() {}

    /// Constructor.
    /// Initializes with parameters for encoding.
    /// @param table %Table identifier
    /// @param range_spec %Range specification
    /// @param range_state %Range state
    /// @param needs_compaction Needs compaction flag
    LoadRange(const TableIdentifier &table, const RangeSpec &range_spec,
              const RangeState &range_state, bool needs_compaction)
      : m_table(table), m_range_spec(range_spec), m_range_state(range_state),
        m_needs_compaction(needs_compaction) { }

    /// Gets table identifier
    /// @return %Table identifier
    const TableIdentifier &table() { return m_table; }

    /// Gets range specification
    /// @return %Range specification
    const RangeSpec &range_spec() { return m_range_spec; }

    /// Gets range state
    /// @return %Range state
    const RangeState &range_state() { return m_range_state; }

    /// Gets <i>needs compaction</i> flag
    /// @return <i>needs compaction</i> flag
    bool needs_compaction() { return m_needs_compaction; }

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

    /// %Range specification
    RangeSpec m_range_spec;

    /// %Range state
    RangeState m_range_state;

    /// <i>needs compaction</i> flag
    bool m_needs_compaction {};
  };

  /// @}

}}}}}

#endif // Hypertable_Lib_RangeServer_Request_Parameters_LoadRange_h
