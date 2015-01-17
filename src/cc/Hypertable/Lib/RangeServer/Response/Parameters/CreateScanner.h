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
/// Declarations for CreateScanner response parameters.
/// This file contains declarations for CreateScanner, a class for encoding
/// and decoding response paramters from the <i>acknowledge load</i>
/// %RangeServer function.

#ifndef Hypertable_Lib_RangeServer_Response_Parameters_CreateScanner_h
#define Hypertable_Lib_RangeServer_Response_Parameters_CreateScanner_h

#include <Hypertable/Lib/ProfileDataScanner.h>

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

  /// %Response parameters for <i>acknowledge load</i> function.
  class CreateScanner : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    CreateScanner() {}

    /// Constructor.
    /// Initializes with parameters for encoding.
    /// @param id Scanner ID
    /// @param skipped_rows Count of rows skipped
    /// @param skipped_cells Count of cells skipped
    /// @param more Flag indicating more data to follow
    /// @param profile_data Profile data
    CreateScanner(int32_t id, int32_t skipped_rows, int32_t skipped_cells,
                  bool more, ProfileDataScanner &profile_data)
      : m_id(id), m_skipped_rows(skipped_rows), m_skipped_cells(skipped_cells),
        m_more(more), m_profile_data(profile_data) {}
    
    /// Gets scanner ID
    /// @return Scanner ID
    int32_t id() { return m_id; }

    /// Gets skipped row count
    /// @return Skipped row count
    int32_t skipped_rows() { return m_skipped_rows; }

    /// Gets skipped cell count
    /// @return Skipped cell count
    int32_t skipped_cells() { return m_skipped_cells; }

    /// Gets profile data
    /// @return Profile data
    const ProfileDataScanner &profile_data() { return m_profile_data; }

    /// Gets <i>more</i> flag
    /// @return <i>more</i> flag
    bool more() { return m_more; }

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

    /// Scanner ID
    int32_t m_id;

    /// Skipped row count
    int32_t m_skipped_rows;

    /// Skipped cell count
    int32_t m_skipped_cells;

    /// Flag indicating if more data to be fetched
    bool m_more;

    /// Profile data
    ProfileDataScanner m_profile_data;

  };

  /// @}

}}}}}

#endif // Hypertable_Lib_RangeServer_Response_Parameters_CreateScanner_h
