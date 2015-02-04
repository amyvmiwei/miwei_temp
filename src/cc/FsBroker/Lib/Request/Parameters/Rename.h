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
/// Declarations for Rename request parameters.
/// This file contains declarations for Rename, a class for encoding and
/// decoding paramters to the <i>rename</i> file system broker function.

#ifndef FsBroker_Lib_Request_Parameters_Rename_h
#define FsBroker_Lib_Request_Parameters_Rename_h

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

  /// %Request parameters for <i>rename</i> requests.
  class Rename : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    Rename() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Sets #m_from to
    /// <code>from</code> and #m_to to <code>to</code>.
    /// @param from Original file name
    /// @param to New file name
    Rename(const String &from, const String &to) : m_from(from), m_to(to) {}

    /// Gets original file name
    /// @return Original file name
    const char *get_from() { return m_from.c_str(); }

    /// Gets new file name
    /// @return New file name
    const char *get_to() { return m_to.c_str(); }

  private:

    uint8_t encoding_version() const override;

    size_t encoded_length_internal() const override;

    void encode_internal(uint8_t **bufp) const override;

    void decode_internal(uint8_t version, const uint8_t **bufp,
			 size_t *remainp) override;

    /// Original file name
    String m_from;

    /// New file name
    String m_to;
  };

  /// @}

}}}}}

#endif // FsBroker_Lib_Request_Parameters_Rename_h
