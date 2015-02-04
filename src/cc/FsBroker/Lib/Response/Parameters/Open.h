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
/// Declarations for Open response parameters.
/// This file contains declarations for Open, a class for encoding and
/// decoding paramters to the <i>open</i> file system broker function.

#ifndef FsBroker_Lib_Response_Parameters_Open_h
#define FsBroker_Lib_Response_Parameters_Open_h

#include <Common/Serializable.h>

#include <string>

using namespace std;

namespace Hypertable {
namespace FsBroker {
namespace Lib {
namespace Response {
namespace Parameters {

  /// @addtogroup FsBrokerLibResponseParameters
  /// @{

  /// %Response parameters for <i>open</i> requests.
  class Open : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    Open() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Sets #m_fd to <code>fd</code>
    /// @param fd File descriptor
    Open(int32_t fd) : m_fd(fd) {}

    /// Gets file descriptor
    /// @return File descriptor
    int32_t get_fd() { return m_fd; }

  private:

    uint8_t encoding_version() const override;

    size_t encoded_length_internal() const override;

    void encode_internal(uint8_t **bufp) const override;

    void decode_internal(uint8_t version, const uint8_t **bufp,
			 size_t *remainp) override;

    /// File descriptor
    int32_t m_fd;

  };

  /// @}

}}}}}

#endif // FsBroker_Lib_Response_Parameters_Open_h
