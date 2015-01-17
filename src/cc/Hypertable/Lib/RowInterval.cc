/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

#include "RowInterval.h"

#include <Common/Logger.h>
#include <Common/Serialization.h>

#include <cstring>
#include <iostream>

using namespace Hypertable;
using namespace Hypertable::Lib;
using namespace std;

uint8_t RowInterval::encoding_version() const {
  return 1;
}

size_t RowInterval::encoded_length_internal() const {
  return 2 + Serialization::encoded_length_vstr(start) +
    Serialization::encoded_length_vstr(end);
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
/// <td>bool</td>
/// <td>Start row inclusive flag</td>
/// </tr>
/// <tr>
/// <td>vstr</td>
/// <td>End row</td>
/// </tr>
/// <tr>
/// <td>bool</td>
/// <td>End row inclusive flag</td>
/// </tr>
/// </table>
void RowInterval::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, start);
  Serialization::encode_bool(bufp, start_inclusive);
  Serialization::encode_vstr(bufp, end);
  Serialization::encode_bool(bufp, end_inclusive);
}

void RowInterval::decode_internal(uint8_t version, const uint8_t **bufp,
                                  size_t *remainp) {
  HT_TRY("decoding row interval",
    start = Serialization::decode_vstr(bufp, remainp);
    start_inclusive = Serialization::decode_bool(bufp, remainp);
    end = Serialization::decode_vstr(bufp, remainp);
    end_inclusive = Serialization::decode_bool(bufp, remainp));
}

const string RowInterval::render_hql() const {
  string hql;
  hql.reserve( (start ? strlen(start) : 0) + (end ? strlen(end) : 0) + 8);
  if (start && *start) {
    hql.append("\"");
    hql.append(start);
    hql.append("\"");
    if (start_inclusive)
      hql.append(" <= ");
    else
      hql.append(" < ");
  }
  hql.append("ROW");
  if (end && *end) {
    if (end_inclusive)
      hql.append(" <= ");
    else
      hql.append(" < ");
    hql.append("\"");
    hql.append(end);
    hql.append("\"");
  }
  return hql;
}

/** @relates RowInterval */
ostream &Hypertable::Lib::operator<<(ostream &os, const RowInterval &ri) {
  os <<"{RowInterval: ";
  if (ri.start)
    os << "\"" << ri.start << "\"";
  else
    os << "NULL";
  if (ri.start_inclusive)
    os << " <= row ";
  else
    os << " < row ";
  if (ri.end_inclusive)
    os << "<= ";
  else
    os << "< ";
  if (ri.end)
    os << "\"" << ri.end << "\"";
  else
    os << "0xff 0xff";
  os << "}";
  return os;
}
