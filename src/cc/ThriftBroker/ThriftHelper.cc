/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#include <Common/Compat.h>

#include "ThriftHelper.h"

#include <Common/TimeInline.h>

#include <iostream>

namespace Hypertable { namespace ThriftGen {

std::ostream &operator<<(std::ostream &out, const CellAsArray &cell) {
  size_t len = cell.size();

  out <<"CellAsArray(";

  if (len > 0)
    out <<" row='"<< cell[0] <<"'";

  if (len > 1)
    out <<" cf='"<< cell[1] <<"'";

  if (len > 2)
    out <<" cq='"<< cell[2] <<"'";

  if (len > 3)
    out <<" value='"<< cell[3] <<"'";

  if (len > 4)
    out << " timestamp="<< cell[4];

  if (len > 5)
    out <<" revision="<< cell[5];

  if (len > 6)
    out << " flag="<< cell[6];

  return out <<")";
}

// must be synced with AUTO_ASSIGN in Hypertable/Lib/KeySpec.h
const int64_t AUTO_ASSIGN = INT64_MIN + 2;

Cell
make_cell(const char *row, const char *cf, const char *cq,
          const std::string &value, int64_t ts, int64_t rev,
          KeyFlag::type flag) {
  Cell cell;

  cell.key.row = row;
  cell.key.column_family = cf;
  cell.key.timestamp = ts;
  cell.key.revision = rev;
  cell.key.flag = flag;
  cell.key.__isset.row = cell.key.__isset.column_family = cell.key.__isset.timestamp
      = cell.key.__isset.revision = cell.key.__isset.flag = true;

  if (cq) {
    cell.key.column_qualifier = cq;
    cell.key.__isset.column_qualifier = true;
  }
  if (value.size()) {
    cell.value = value;
    cell.__isset.value = true;
  }
  return cell;
}

Cell
make_cell(const char *row, const char *cf, const char *cq,
          const std::string &value, const char *ts, const char *rev,
          KeyFlag::type flag) {
  int64_t revn = rev ? atoll(rev) : AUTO_ASSIGN;

  if (ts)
    return make_cell(row, cf, cq, value, parse_ts(ts), revn, flag);
  else
    return make_cell(row, cf, cq, value, AUTO_ASSIGN, revn, flag);
}

}} // namespace Hypertable::Thrift

