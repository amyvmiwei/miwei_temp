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
/// Declarations for Mkdirs request parameters.
/// This file contains declarations for Mkdirs, a class for encoding and
/// decoding paramters to the <i>mkdirs</i> file system broker function.

#ifndef FsBroker_Lib_Request_Parameters_Mkdirs_h
#define FsBroker_Lib_Request_Parameters_Mkdirs_h

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

  /// %Request parameters for <i>mkdirs</i> requests.
  class Mkdirs : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    Mkdirs() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Sets #m_dirname to
    /// <code>dirname</code>.
    /// @param dirname Directory name
    Mkdirs(const String &dirname) : m_dirname(dirname) {}

    /// Gets directory name
    /// @return Directory name
    const char *get_dirname() { return m_dirname.c_str(); }

  private:

    uint8_t encoding_version() const override;

    size_t encoded_length_internal() const override;

    void encode_internal(uint8_t **bufp) const override;

    void decode_internal(uint8_t version, const uint8_t **bufp,
			 size_t *remainp) override;

    /// Directory name
    String m_dirname;
  };

  /// @}

}}}}}

#endif // FsBroker_Lib_Request_Parameters_Mkdirs_h
