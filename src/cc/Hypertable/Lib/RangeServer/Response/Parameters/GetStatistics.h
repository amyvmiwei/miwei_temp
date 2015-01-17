/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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
/// Declarations for GetStatistics response parameters.
/// This file contains declarations for GetStatistics, a class for encoding
/// and decoding response paramters from the <i>get statistics</i>
/// %RangeServer function.

#ifndef Hypertable_Lib_RangeServer_Response_Parameters_GetStatistics_h
#define Hypertable_Lib_RangeServer_Response_Parameters_GetStatistics_h

#include <Hypertable/Lib/StatsRangeServer.h>

#include <Common/Serializable.h>

#include <map>
#include <string>

using namespace std;

namespace Hypertable {
namespace Lib {
namespace RangeServer {
namespace Response {
namespace Parameters {

  /// @addtogroup libHypertableRangeServerResponseParameters
  /// @{

  /// %Response parameters for <i>get statistics</i> function.
  class GetStatistics : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    GetStatistics() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Initializes #m_stats with
    /// <code>stats</code>.
    /// @param stats Range server statistics
    GetStatistics(const StatsRangeServer &stats) : m_stats(stats) {}

    /// Gets range server statistics
    /// @return Range server statistics
    const StatsRangeServer &stats() { return m_stats; }

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

    /// Range server statistics
    StatsRangeServer m_stats;

  };

  /// @}

}}}}}

#endif // Hypertable_Lib_RangeServer_Response_Parameters_GetStatistics_h
