/* -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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
/// Declarations for CellStoreFactory.
/// This file contains the type declarations for CellStoreFactory, an class that
/// provides an interface for creating CellStore objects from cell store files.

#ifndef HYPERTABLE_CELLSTOREFACTORY_H
#define HYPERTABLE_CELLSTOREFACTORY_H

#include <Hypertable/RangeServer/CellStore.h>
#include <Hypertable/RangeServer/CellStoreTrailer.h>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Factory class for creating CellStore objects.
  class CellStoreFactory {
  public:

    /// Creates a CellStore object from a given cell store file.
    /// @param name Pathname of cell store file
    /// @param start_row Starting scope
    /// @param end_row Ending scope
    /// @return Pointer to newly allocated CellStore object
    static CellStore *open(const String &name,
                           const char *start_row, const char *end_row);
  };

  /// @}

}

#endif // HYPERTABLE_CELLSTOREFACTORY_H
