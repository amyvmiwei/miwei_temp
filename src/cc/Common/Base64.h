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
/// Declarations for Base64.
/// This file contains type declarations for Base64, a static class that
/// provides base64 encoding and decoding functions.

#ifndef Common_Base64_h
#define Common_Base64_h

namespace Hypertable {

  using namespace std;

  /// @addtogroup Common
  /// @{

  /// Provides base64 encoding and decoding functions.
  class Base64 {
  public:

    /// Encodes message in base64.
    /// @param message Message to encode
    /// @param newlines Insert newlines after every 64 characters
    /// @return Message encoded in base64
    static const string encode(const string &message, bool newlines=false);

    /// Decodes base64 message.
    /// @param message Message (in base64) to decode.
    /// @param newlines Message was encoded with newlines
    /// @return Decoded message
    static const string decode(const string &message, bool newlines=false);

  };

  /// @}
}

#endif // Common_Base64_h
