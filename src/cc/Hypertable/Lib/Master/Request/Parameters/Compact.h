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
/// Declarations for Compact request parameters.
/// This file contains declarations for Compact, a class for encoding and
/// decoding paramters to the <i>compact</i> %Master operation.

#ifndef Hypertable_Lib_Master_Request_Parameters_Compact_h
#define Hypertable_Lib_Master_Request_Parameters_Compact_h

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

  /// %Request parameters for <i>compact</i> operation.
  class Compact : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    Compact() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Sets #m_table_name to
    /// <code>table_name</code>, #m_row to <code>row</code>, and #m_range_types
    /// to <code>range_types</code>.
    /// @param table_name Name of table to be compacted
    /// @param row Row identifying range to be compacted
    /// @param range_types Bit mask of range types to be compacted
    Compact(const std::string &table_name, const std::string &row,
            int32_t range_types);

    /// Gets name of table to compact.
    /// @return Name of table to compact
    const std::string& table_name() const { return m_table_name; }

    /// Gets row identifying range to be compacted.
    /// @return Row identifying range to be compacted.
    const std::string& row() const { return m_row; }

    /// Gets range types to be compacted.
    /// @return Range types to be compacted.
    int32_t range_types() { return m_range_types; }

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

    /// Name of table to be compacted
    std::string m_table_name;

    /// Row identifying range to be compacted
    std::string m_row;

    /// Bit mask of range types to be compacted
    int32_t m_range_types {};
  };

  /// @}

}}}}}

#endif // Hypertable_Lib_Master_Request_Parameters_Compact_h
