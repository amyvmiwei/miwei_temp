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
/// Declarations for NamespaceFlag.
/// This file contains declarations for NamespaceFlag, a class encapsulating
/// namespace operation bit flags.

#ifndef Hypertable_Lib_Master_NamespaceFlag_h
#define Hypertable_Lib_Master_NamespaceFlag_h

#include <string>

namespace Hypertable {
namespace Lib {
namespace Master {

  /// @addtogroup libHypertableMaster
  /// @{

  /// %Namespace operation bit flags.
  class NamespaceFlag {
  public:
    /// Enumeration for namespace operation flags.
    enum {
      /// Create intermediate namespaces
      CREATE_INTERMEDIATE = 0x0001,
      /// Perform operation if namespace exists
      IF_EXISTS = 0x0002,
      /// Perform operation if namespace does not exist
      IF_NOT_EXISTS = 0x0004
    };

    /// Converts flags to human readable string.
    /// @param flags Bit mask of namespace operation flags
    /// @return Human readable representation of namespace operation bit flags
    static std::string to_str(int flags);
  };

  /// @}

}}}

#endif // Hypertable_Lib_Master_NamespaceFlag_h
