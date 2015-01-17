/*
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
/// Definitions for QualifiedRangeSpec and QualifiedRangeSpecManaged.
/// This file contains definitions for QualifiedRangeSpec and
/// QualifiedRangeSpecManaged, classes for holding a qualified range
/// specification, which is a range specification with the owning table
/// identifier.

#include <Common/Compat.h>

#include "QualifiedRangeSpec.h"

#include <Common/Logger.h>
#include <Common/Properties.h>
#include <Common/Serialization.h>
#include <Common/StringExt.h>
#include <Common/md5.h>

using namespace std;
using namespace Hypertable;
using namespace Serialization;

bool QualifiedRangeSpec::is_root() const {
  bool start_null = (range.start_row == 0) || (strlen(range.start_row)==0);
  return (table.is_metadata() && start_null);
}

bool QualifiedRangeSpec::operator<(const QualifiedRangeSpec &other) const {
  if (!strcmp(table.id, other.table.id))
    return (range < other.range);
  else
    return strcmp(table.id, other.table.id) < 0;
}

bool QualifiedRangeSpec::operator==(const QualifiedRangeSpec &other) const {
  return (table == other.table && range == other.range);
}


uint8_t QualifiedRangeSpec::encoding_version() const {
  return 1;
}

size_t QualifiedRangeSpec::encoded_length_internal() const {
  return table.encoded_length() + range.encoded_length();
}

/// @details
/// Encoding is as follows:
/// <table>
/// <tr>
/// <th>Encoding</th>
/// <th>Description</th>
/// </tr>
/// <tr>
/// <td>TableIdentifier</td>
/// <td>Table identifier</td>
/// </tr>
/// <tr>
/// <td>RangeSpec</td>
/// <td>Range specification</td>
/// </tr>
/// </table>
void QualifiedRangeSpec::encode_internal(uint8_t **bufp) const {
  table.encode(bufp);
  range.encode(bufp);
}

void QualifiedRangeSpec::decode_internal(uint8_t version, const uint8_t **bufp,
                                         size_t *remainp) {
  HT_TRY("decoding qualified range spec ",
    table.decode(bufp, remainp);
    range.decode(bufp, remainp));
}

bool QualifiedRangeSpecManaged::operator<(const QualifiedRangeSpecManaged &other) const {
  if (table == other.table)
    return (range < other.range);
  else
    return (table < other.table);
}

void QualifiedRangeSpecManaged::decode_internal(uint8_t version, const uint8_t **bufp,
                                             size_t *remainp) {
  QualifiedRangeSpec::decode_internal(version, bufp, remainp);
  *this = *this;
}

/** @relates QualifiedRangeSpec */
ostream &Hypertable::operator<<(ostream &os, const QualifiedRangeSpec &qualified_range) {
  os << qualified_range.table << qualified_range.range;
  return os;
}

/** @relates QualifiedRangeSpecManaged */
ostream &Hypertable::operator<<(ostream &os, const QualifiedRangeSpecManaged &qualified_range) {
  os << qualified_range.m_table << qualified_range.m_range;
  return os;
}

