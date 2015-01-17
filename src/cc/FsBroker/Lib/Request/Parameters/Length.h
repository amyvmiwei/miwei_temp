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
/// Declarations for Length request parameters.
/// This file contains declarations for Length, a class for encoding and
/// decoding paramters to the <i>length</i> file system broker function.

#ifndef FsBroker_Lib_Request_Parameters_Length_h
#define FsBroker_Lib_Request_Parameters_Length_h

#include <Common/Serializable.h>

#include <string>

using namespace std;

namespace Hypertable {
namespace FsBroker {
namespace Lib {
namespace Request {
namespace Parameters {

  /// @addtogroup FsBrokerLibRequestParameters
  /// @{

  /// %Request parameters for <i>length</i> requests.
  class Length : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    Length() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Sets #m_fname to
    /// <code>fname</code> and #m_accurate to <code>accurate</code>
    /// @param fname File name
    /// @param accurate Flag indicating that accurate length is desired
    Length(const String &fname, bool accurate)
      : m_fname(fname), m_accurate(accurate) {}

    /// Gets file name
    /// @return File name
    const char *get_fname() { return m_fname.c_str(); }

    /// Gets accureate flag
    /// @return Accurate flag
    bool get_accurate() { return m_accurate; }

  private:

    uint8_t encoding_version() const override;

    size_t encoded_length_internal() const override;

    void encode_internal(uint8_t **bufp) const override;

    void decode_internal(uint8_t version, const uint8_t **bufp,
			 size_t *remainp) override;

    /// File name
    String m_fname;

    /// Accurate flag
    bool m_accurate;
  };

  /// @}

}}}}}

#endif // FsBroker_Lib_Request_Parameters_Length_h
