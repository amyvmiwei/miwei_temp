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
/// Declarations for Dump request parameters.
/// This file contains declarations for Dump, a class for encoding and
/// decoding paramters to the <i>dump</i> %RangeServer function.

#ifndef Hypertable_Lib_RangeServer_Request_Parameters_Dump_h
#define Hypertable_Lib_RangeServer_Request_Parameters_Dump_h

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

  /// %Request parameters for <i>dump</i> function.
  class Dump : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    Dump() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Initializes #m_fname with
    /// <code>fname</code> and #m_no_keys with <code>no_keys</code>.
    /// @param fname Output file name for dump
    /// @param no_keys Flag indicating cell cache keys should not be dumped
    Dump(const std::string &fname, bool no_keys)
      : m_fname(fname), m_no_keys(no_keys) { }

    /// Gets output file name
    /// @return Output file name
    const char *fname() { return m_fname.c_str(); }

    /// Gets <i>no keys</i> flag
    /// @return <i>no keys</i> flag
    bool no_keys() { return m_no_keys; }

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

    /// Output file name
    std::string m_fname;

    /// Flag indicating cell cache keys should not be dumped
    bool m_no_keys {};
  };

  /// @}

}}}}}

#endif // Hypertable_Lib_RangeServer_Request_Parameters_Dump_h
