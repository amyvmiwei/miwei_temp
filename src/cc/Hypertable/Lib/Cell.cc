/**
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

#include "Common/Compat.h"
#include "Common/Logger.h"
#include "Cell.h"

namespace Hypertable {

std::ostream &operator<<(std::ostream &os, const Cell &cell) {
  os <<"{Cell:";

  HT_DUMP_CSTR(os, key, cell.row_key);
  HT_DUMP_CSTR(os, cf, cell.column_family);
  HT_DUMP_CSTR(os, cq, cell.column_qualifier);
  HT_DUMP_CSTR(os, val, cell.value);

  os <<" len="<< cell.value_len
     <<" ts="<< cell.timestamp
     <<" flag=";
  if (cell.flag==FLAG_DELETE_ROW)
    os << "DELETE_ROW";
  else if (cell.flag==FLAG_DELETE_COLUMN_FAMILY)
    os << "DELETE_COLUMN_FAMILY";
  else if (cell.flag==FLAG_DELETE_CELL)
    os << "DELETE_CELL";
  else if (cell.flag == FLAG_DELETE_CELL_VERSION)
    os << "DELETE_CELL_VERSION";
  else if (cell.flag==FLAG_INSERT)
    os << "FLAG_INSERT";
  else
    os << cell.flag << " (unrecognized)";

  os <<'}';
  return os;
}

} // namespace Hypertable
