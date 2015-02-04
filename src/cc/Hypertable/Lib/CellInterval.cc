/*
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

#include <Common/Compat.h>

#include "CellInterval.h"

#include <cstring>
#include <iostream>

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib;
using namespace std;

uint8_t CellInterval::encoding_version() const {
  return 1;
}

size_t CellInterval::encoded_length_internal() const {
  return 2 + Serialization::encoded_length_vstr(start_row) +
    Serialization::encoded_length_vstr(start_column) +
    Serialization::encoded_length_vstr(end_row) +
    Serialization::encoded_length_vstr(end_column);
}

/// @details
/// Encoding is as follows:
/// <table>
/// <tr>
/// <th>Encoding</th>
/// <th>Description</th>
/// </tr>
/// <tr>
/// <td>vstr</td>
/// <td>Start row</td>
/// </tr>
/// <tr>
/// <td>vstr</td>
/// <td>Start column</td>
/// </tr>
/// <tr>
/// <td>bool</td>
/// <td>Start row inclusive flag</td>
/// </tr>
/// <tr>
/// <td>vstr</td>
/// <td>End row</td>
/// </tr>
/// <tr>
/// <td>vstr</td>
/// <td>End column</td>
/// </tr>
/// <tr>
/// <td>bool</td>
/// <td>End row inclusive flag</td>
/// </tr>
/// </table>
void CellInterval::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, start_row);
  Serialization::encode_vstr(bufp, start_column);
  Serialization::encode_bool(bufp, start_inclusive);
  Serialization::encode_vstr(bufp, end_row);
  Serialization::encode_vstr(bufp, end_column);
  Serialization::encode_bool(bufp, end_inclusive);
}

void CellInterval::decode_internal(uint8_t version, const uint8_t **bufp,
                                      size_t *remainp) {
  HT_TRY("decoding cell interval",
    start_row = Serialization::decode_vstr(bufp, remainp);
    start_column = Serialization::decode_vstr(bufp, remainp);
    start_inclusive = Serialization::decode_bool(bufp, remainp);
    end_row = Serialization::decode_vstr(bufp, remainp);
    end_column = Serialization::decode_vstr(bufp, remainp);
    end_inclusive = Serialization::decode_bool(bufp, remainp));
}

const string CellInterval::render_hql() const {
  string hql;
  hql.reserve( (start_row ? strlen(start_row) : 0) +
               (start_column ? strlen(start_column) : 0) +
               (end_row ? strlen(end_row) : 0) +
               (end_column ? strlen(end_column) : 0) + 8);
  if (start_row && *start_row) {
    hql.append("\"");
    hql.append(start_row);
    hql.append("',\"");
    if (start_column && *start_column)
      hql.append(start_column);
    hql.append("',");
    if (start_inclusive)
      hql.append(" <= ");
    else
      hql.append(" < ");
  }
  hql.append("CELL");
  if (end_row && *end_row) {
    if (end_inclusive)
      hql.append(" <= ");
    else
      hql.append(" < ");
    hql.append("\"");
    hql.append(end_row);
    hql.append("',\"");
    if (end_column && *end_column)
      hql.append(end_column);
    hql.append("',");
  }
  return hql;
}

/** @relates CellInterval */
ostream &Hypertable::Lib::operator<<(ostream &os, const CellInterval &ci) {
  os <<"{CellInterval: ";
  if (ci.start_row)
    os << "\"" << ci.start_row << "\",\"" << ci.start_column << "\"";
  else
    os << "NULL";
  if (ci.start_inclusive)
    os << " <= cell ";
  else
    os << " < cell ";
  if (ci.end_inclusive)
    os << "<= ";
  else
    os << "< ";
  if (ci.end_row)
    os << "\"" << ci.end_row << "\",\"" << ci.end_column << "\"";
  else
    os << "0xff 0xff";
  os << "}";
  return os;
}
