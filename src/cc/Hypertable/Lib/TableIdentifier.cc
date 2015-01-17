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

/// @file
/// Definitions for TableIdentifier and TableIdentifierManaged.
/// This file contains definitions for TableIdentifier and
/// TableIdentifierManaged, classes for identifying a table.

#include <Common/Compat.h>

#include "TableIdentifier.h"

#include <Common/Serialization.h>

using namespace Hypertable;
using namespace std;

const char *TableIdentifier::METADATA_ID = "0/0";
const char *TableIdentifier::METADATA_NAME= "sys/METADATA";
const int TableIdentifier::METADATA_ID_LENGTH = 3;

bool TableIdentifier::operator==(const TableIdentifier &other) const {
  if (id == 0 || other.id == 0) {
    if (id != other.id)
      return false;
  }
  if (strcmp(id, other.id) ||
      generation != other.generation)
    return false;
  return true;
}

bool TableIdentifier::operator!=(const TableIdentifier &other) const {
  return !(*this == other);
}

bool TableIdentifier::operator<(const TableIdentifier &other) const {
  if (id == 0 || other.id == 0) {
    if (other.id != 0)
      return true;
    else if (id != 0)
      return false;
  }
  int cmpval = strcmp(id, other.id);

  if (cmpval < 0 ||
      (cmpval == 0 && generation < other.generation))
    return true;
  return false;
}

uint8_t TableIdentifier::encoding_version() const {
  return 1;
}

size_t TableIdentifier::encoded_length_internal() const {
  return Serialization::encoded_length_vstr(id) + 8;
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
/// <td>ID path</td>
/// </tr>
/// <tr>
/// <td>vstr</td>
/// <td>Generation</td>
/// </tr>
/// </table>
void TableIdentifier::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, id);
  Serialization::encode_i64(bufp, generation);
}

void TableIdentifier::decode_internal(uint8_t version, const uint8_t **bufp,
                                      size_t *remainp) {
  id = Serialization::decode_vstr(bufp, remainp);
  generation = Serialization::decode_i64(bufp, remainp);
}

void TableIdentifierManaged::decode_internal(uint8_t version, const uint8_t **bufp,
                                             size_t *remainp) {
  TableIdentifier::decode_internal(version, bufp, remainp);
  *this = *this;
}

/** @relates TableIdentifier */
ostream &Hypertable::operator<<(ostream &os, const TableIdentifier &tid) {
  os <<"{TableIdentifier: id='"<< tid.id
     <<"' generation="<< tid.generation <<"}";
  return os;
}
