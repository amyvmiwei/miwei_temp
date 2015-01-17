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

#include "ColumnPredicate.h"

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib;
using namespace std;

uint8_t ColumnPredicate::encoding_version() const {
  return 1;
}

size_t ColumnPredicate::encoded_length_internal() const {
  return 4 + Serialization::encoded_length_vstr(column_family) +
    Serialization::encoded_length_vstr(column_qualifier_len) +
    Serialization::encoded_length_vstr(value_len);
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
/// <td>Column family</td>
/// </tr>
/// <tr>
/// <td>vstr</td>
/// <td>Column qualifier</td>
/// </tr>
/// <tr>
/// <td>vstr</td>
/// <td>Value</td>
/// </tr>
/// <tr>
/// <td>i32</td>
/// <td>Operation</td>
/// </tr>
/// </table>
void ColumnPredicate::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, column_family);
  Serialization::encode_vstr(bufp, column_qualifier, column_qualifier_len);
  Serialization::encode_vstr(bufp, value, value_len);
  Serialization::encode_i32(bufp, operation);
}

void ColumnPredicate::decode_internal(uint8_t version, const uint8_t **bufp,
                                      size_t *remainp) {
  HT_TRY("decoding column predicate",
         column_family = Serialization::decode_vstr(bufp, remainp);
         column_qualifier = Serialization::decode_vstr(bufp, remainp, &column_qualifier_len);
         value = Serialization::decode_vstr(bufp, remainp, &value_len);
         operation = Serialization::decode_i32(bufp, remainp));
}

const string ColumnPredicate::render_hql() const {
  bool exists = (operation & ColumnPredicate::VALUE_MATCH) == 0;
  string hql;
  hql.reserve(strlen(column_family) + column_qualifier_len + value_len + 16);
  if (exists)
    hql.append("Exists(");
  hql.append(column_family);
  if (column_qualifier_len) {
    hql.append(":");
    if (operation & ColumnPredicate::QUALIFIER_EXACT_MATCH)
      hql.append(column_qualifier);
    else if (operation & ColumnPredicate::QUALIFIER_PREFIX_MATCH) {
      hql.append(column_qualifier);
      hql.append("*");
    }
    else if (operation & ColumnPredicate::QUALIFIER_REGEX_MATCH) {
      hql.append("/");
      hql.append(column_qualifier);
      hql.append("/");
    }
  }
  if (exists) {
    hql.append(")");
    return hql;
  }
  HT_ASSERT(value);
  if (operation & ColumnPredicate::EXACT_MATCH)
    hql.append(" = \"");
  else if (operation & ColumnPredicate::PREFIX_MATCH)
    hql.append(" =^ \"");
  else if (operation & ColumnPredicate::REGEX_MATCH)
    hql.append(" =~ /");
  hql.append(value);
  if (operation & ColumnPredicate::REGEX_MATCH)
    hql.append("/");
  else
    hql.append("\"");
  return hql;
}

/** @relates ColumnPredicate */
std::ostream &Hypertable::Lib::operator<<(std::ostream &os, const ColumnPredicate &cp) {
  os << "{ColumnPredicate";
  if (cp.column_family)
    os << " column_family=" << cp.column_family;
  if (cp.column_qualifier)
    os << " column_qualifier=" << cp.column_qualifier << ",len=" << cp.column_qualifier_len;
  if (cp.value)
    os << " value=" << cp.value << ",len=" << cp.value_len;
  os << " operation=" << cp.operation << "}";
  return os;
}
