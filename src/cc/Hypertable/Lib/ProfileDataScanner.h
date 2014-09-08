/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3
 * of the License.
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
/// Declarations for ProfileDataScanner.
/// This file contains type declarations for ProfileDataScanner, a calls that
/// holds profile information for a scanner request.

#ifndef Hypertable_Lib_ProfileDataScanner_h
#define Hypertable_Lib_ProfileDataScanner_h

#include <Common/Serializable.h>

#include <cstdint>
#include <set>
#include <string>

namespace Hypertable {

  /// @addtogroup libHypertable
  /// @{

  /// Scanner profile data.
  /// This class is used to store scanner profile data.
  class ProfileDataScanner : public Serializable {
  public:

    /// Returns serialized object length
    /// @return Serialized object length
    size_t encoded_length() const override;

    /// Writes serialized representation of object to a buffer.
    /// @param bufp Address of destination buffer pointer (advanced by call)
    void encode(uint8_t **bufp) const override;

    /// Reads serialized representation of object from a buffer.
    /// @param bufp Address of destination buffer pointer (advanced by call)
    /// @param remainp Address of integer holding amount of remaining buffer
    void decode(const uint8_t **bufp, size_t *remainp) override;

    ProfileDataScanner &operator+=(const ProfileDataScanner &other);

    ProfileDataScanner &operator-=(const ProfileDataScanner &other);

    std::string to_string();

    int32_t subscanners {};
    int32_t scanblocks {};
    int64_t cells_scanned {};
    int64_t cells_returned {};
    int64_t bytes_scanned {};
    int64_t bytes_returned {};
    int64_t disk_read {};
    std::set<std::string> servers;
  };

  /// @}
}

#endif // Hypertable_Lib_ProfileDataScanner_h
