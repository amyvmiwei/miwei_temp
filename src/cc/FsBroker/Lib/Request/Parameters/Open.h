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
/// Declarations for Open request parameters.
/// This file contains declarations for Open, a class for encoding and
/// decoding paramters to the <i>open</i> file system broker function.

#ifndef FsBroker_Lib_Request_Parameters_Open_h
#define FsBroker_Lib_Request_Parameters_Open_h

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

  /// %Request parameters for <i>open</i> requests.
  class Open : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    Open() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Sets #m_fname to
    /// <code>fname</code>, #m_flags to <code>flags</code>, and #m_bufsz to
    /// <code>bufsz</code>.
    /// @param fname File name
    /// @param flags Open flags
    /// @param bufsz Buffer size
    Open(const String &fname, uint32_t flags, int32_t bufsz)
      : m_fname(fname), m_flags(flags), m_bufsz(bufsz) {}

    /// Returns encoded length of parameters
    /// @return Encoded length of parameters
    size_t encoded_length() const override;

    /// Encodes parameters to buffer
    /// @param bufp Address of buffer to encode parameters to
    /// (advanced by call)
    void encode(uint8_t **bufp) const override;

    /// Decodes parameters from buffer
    /// @param bufp Address of buffer from which to decode parameters
    /// (advanced by call)
    /// @param remainp Address of remaining encoded data in buffer
    /// (advanced by call)
    void decode(const uint8_t **bufp, size_t *remainp) override;

    /// Gets file name
    /// @return File name
    const char *get_fname() { return m_fname.c_str(); }

    /// Gets open flags
    /// @return Open flags
    uint32_t get_flags() { return m_flags; }

    /// Gets buffer size
    /// @return Buffer size
    int32_t get_buffer_size() { return m_bufsz; }

  private:

    /// Returns internal encoded length
    size_t internal_encoded_length() const;

    /// File name
    String m_fname;

    /// Open flags
    uint32_t m_flags;

    /// Buffer size
    int32_t m_bufsz;

  };

  /// @}

}}}}}

#endif // FsBroker_Lib_Request_Parameters_Open_h
