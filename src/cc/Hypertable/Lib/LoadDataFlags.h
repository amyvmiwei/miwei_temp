/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License.
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

#ifndef HYPERTABLE_LOADDATAFLAGS_H
#define HYPERTABLE_LOADDATAFLAGS_H

namespace Hypertable { namespace LoadDataFlags {

  enum {
    DUP_KEY_COLS           = 0x0001,
    NO_ESCAPE              = 0x0002,
    IGNORE_UNKNOWN_COLUMNS = 0x0004,
    SINGLE_CELL_FORMAT     = 0x0008,
    NO_LOG                 = 0x0010
  };

  inline bool duplicate_key_columns(int flags) {
    return (flags & DUP_KEY_COLS) == DUP_KEY_COLS;
  }

  inline bool no_escape(int flags) {
    return (flags & NO_ESCAPE) == NO_ESCAPE;
  }

  inline bool ignore_unknown_cfs(int flags) {
    return (flags & IGNORE_UNKNOWN_COLUMNS) == IGNORE_UNKNOWN_COLUMNS;
  }

  inline bool single_cell_format(int flags) {
    return (flags & SINGLE_CELL_FORMAT) == SINGLE_CELL_FORMAT;
  }

  inline bool no_log(int flags) {
    return (flags & NO_LOG) == NO_LOG;
  }

} }


#endif // HYPERTABLE_LOADDATAFLAGS_H
