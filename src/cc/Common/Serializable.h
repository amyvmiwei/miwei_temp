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
/// Declarations for Serializable.
/// This file contains declarations for Serializable, a mixin class that
/// provides a serialization interface.

#ifndef Hypertable_Common_Serializable_h
#define Hypertable_Common_Serializable_h

#include <Common/Serialization.h>

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  /// Mixin class that provides a serialization interface.
  class Serializable {

  public:

    /// Returns serialized object length
    /// @return Serialized object length
    virtual size_t encoded_length() const = 0;

    /// Writes serialized representation of object to a buffer.
    /// @param bufp Address of destination buffer pointer (advanced by call)
    virtual void encode(uint8_t **bufp) const = 0;

    /// Reads serialized representation of object from a buffer.
    /// @param bufp Address of destination buffer pointer (advanced by call)
    /// @param remainp Address of integer holding amount of remaining buffer
    virtual void decode(const uint8_t **bufp, size_t *remainp) = 0;

  };

  /// @}

}

#endif // Hypertable_Common_Serializable_h
