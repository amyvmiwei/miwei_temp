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
/// Declarations for Regex.
/// This file contains declarations for Regex, a static class containing regular
/// expression utility functions.

#ifndef Common_Regex_h
#define Common_Regex_h

#include <Common/DynamicBuffer.h>

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  /// Provides utility functions for regular expressions.
  class Regex {
  public:

    /// Extracts a fixed prefix from regular expression.
    /// This function extracts a fixed prefix from the regular expression
    /// defined by <code>regex</code> and <code>regex_len</code>.  A candidate
    /// string which maches the regex implies that the string begins with the
    /// fixed prefix.  A fixed prefix can only be extacted from a regular
    /// expression that begins with the '^' character and is followed by some
    /// number of non-qualified, non-meta characters.
    /// @param regex Pointer to regular expression
    /// @param regex_len Length of regular expression
    /// @param output Address of output pointer to beginning of prefix
    /// @param output_len Address of output length to hold length of prefix
    /// @param buf Buffer to hold prefix if expansion was required
    /// @return <i>true</i> if a valid prefix was extracted, <i>false</i>
    /// otherwise.
    static bool extract_prefix(const char *regex, size_t regex_len,
                               const char **output, size_t *output_len,
                               DynamicBuffer &buf);

  };

  /// @}
}

#endif // Common_Regex_h
