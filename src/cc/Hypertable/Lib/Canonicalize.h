/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
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
/// Declarations for Canonicalize.
/// This file contains declarations for Canonicalize, a static class for
/// canonicalizing table and namespace names.

#ifndef Hypertable_Lib_Canonicalize_h
#define Hypertable_Lib_Canonicalize_h

#include <string>

namespace Hypertable {

  /// @addtogroup libHypertable
  /// @{

  /// Static class with methods for canonicalizing table and namespace names.
  class Canonicalize {
  public:

    /// Canonicalizes a table name.
    /// Strips leading and trailing whitespace and ensures that
    /// <code>name</code> begins with a '/' character
    /// @param name Table name to canonicalize
    static void table_name(std::string &name);

    /// Canonicalizes a namespace path.
    /// Strips leading and trailing whitespace and ensures that
    /// <code>path</code> begins with a '/' character
    /// @param path Namespace path to canonicalize
    static void namespace_path(std::string &path);

  };

  /// @}

}

#endif // Hypertable_Lib_Canonicalize_h
