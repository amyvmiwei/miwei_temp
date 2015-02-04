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
/// Declarations for DumpPseudoTable request parameters.
/// This file contains declarations for DumpPseudoTable, a class for encoding and
/// decoding paramters to the <i>dump pseudo table</i> %RangeServer function.

#ifndef Hypertable_Lib_RangeServer_Request_Parameters_DumpPseudoTable_h
#define Hypertable_Lib_RangeServer_Request_Parameters_DumpPseudoTable_h

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

  /// %Request parameters for <i>dump pseudo table</i> function.
  class DumpPseudoTable : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    DumpPseudoTable() {}

    /// Constructor.
    /// Initializes with parameters for encoding.
    /// @param table %Table identifier
    /// @param pseudo_table_name Pseudo table name
    /// @param output_file_name Output file name
    DumpPseudoTable(const TableIdentifier &table,
                    const string &pseudo_table_name,
                    const string &output_file_name)
      : m_table(table), m_pseudo_table_name(pseudo_table_name.c_str()),
        m_output_file_name(output_file_name.c_str()) { }

    /// Gets table identifier
    /// @return %Table identifier
    const TableIdentifier &table() { return m_table; }

    /// Gets pseudo table name
    /// @return Pseudo table name
    const char *pseudo_table_name() { return m_pseudo_table_name; }

    /// Gets output file name
    /// @return Output file name
    const char *output_file_name() { return m_output_file_name; }

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

    /// Pseudo table name
    const char *m_pseudo_table_name;

    /// Output file name
    const char *m_output_file_name;

  };

  /// @}

}}}}}

#endif // Hypertable_Lib_RangeServer_Request_Parameters_DumpPseudoTable_h
