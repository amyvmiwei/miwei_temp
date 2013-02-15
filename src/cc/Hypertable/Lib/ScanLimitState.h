/* -*- c++ -*-
 * Copyright (C) 2007-2013 Hypertable, Inc.
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

/** @file
 * Declaration of ScanLimitState
 * This file contains the type declaration for ScanLimitState, a class to
 * track limits during a scan.
 */

#ifndef HYPERTABLE_SCANLIMITSTATE_H
#define HYPERTABLE_SCANLIMITSTATE_H

#include "ScanSpec.h"

namespace Hypertable {

/** Tracks row and cell limits used to enforce scan limit predicates.
 */
class ScanLimitState {
public:
  ScanLimitState(const ScanSpec &spec) 
    : row_limit(spec.row_limit), rows_seen(0), cell_limit(spec.cell_limit),
      cells_seen(0) { }
  String last_row;   //!< Last row processed
  size_t row_limit;  //!< Row limit
  size_t rows_seen;  //!< Number of complete rows seen
  size_t cell_limit; //!< Cell limit
  size_t cells_seen; //!< Cells seen
};

} // namespace Hypertable

#endif // HYPERTABLE_SCANLIMITSTATE_H
