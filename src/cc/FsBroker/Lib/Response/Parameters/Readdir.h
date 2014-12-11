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
/// Declarations for Readdir response parameters.
/// This file contains declarations for Readdir, a class for encoding and
/// decoding paramters to the <i>readdir</i> file system broker function.

#ifndef FsBroker_Lib_Response_Parameters_Readdir_h
#define FsBroker_Lib_Response_Parameters_Readdir_h

#include <Common/Filesystem.h>
#include <Common/Serializable.h>

#include <string>
#include <vector>

using namespace std;

namespace Hypertable {
namespace FsBroker {
namespace Lib {
namespace Response {
namespace Parameters {

  /// @addtogroup FsBrokerLibResponseParameters
  /// @{

  /// %Response parameters for <i>readdir</i> requests.
  class Readdir : public Serializable {
  public:

    /// Constructor.
    /// Empty initialization for decoding.
    Readdir() {}

    /// Constructor.
    /// Initializes with parameters for encoding.  Sets #m_listing to
    /// <code>listing</code>.
    /// @param listing Directory listing
    Readdir(std::vector<Filesystem::Dirent> &listing) : m_listing(listing) {}

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

    /// Gets directory listing
    /// @return Directory listing
    void get_listing(std::vector<Filesystem::Dirent> &listing) {
      listing = m_listing;
    }

  private:

    /// Returns internal encoded length
    size_t internal_encoded_length() const;

    /// Directory listing
    std::vector<Filesystem::Dirent> m_listing;
  };

  /// @}

}}}}}

#endif // FsBroker_Lib_Response_Parameters_Readdir_h
