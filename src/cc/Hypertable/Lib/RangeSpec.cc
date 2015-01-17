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
/// Definitions for RangeSpec and RangeSpecManaged.
/// This file contains definitions for RangeSpec and RangeSpecManaged, classes
/// for identifying a range.

#include <Common/Compat.h>

#include "RangeSpec.h"

#include <Common/Logger.h>
#include <Common/Properties.h>
#include <Common/Serialization.h>
#include <Common/StringExt.h>
#include <Common/md5.h>

using namespace std;
using namespace Hypertable;
using namespace Serialization;

String RangeSpec::type_str(int type) {
  switch(type) {
  case (ROOT):
    return (String) "root";
  case (METADATA):
    return (String) "metadata";
  case (SYSTEM):
    return (String) "system";
  case (USER):
    return "user";
  case (UNKNOWN):
    return "unknown";
  }
  return "unknown";
}

bool RangeSpec::operator==(const RangeSpec &other) const {

  bool start_null = (start_row == 0) || (strlen(start_row)==0);
  bool other_start_null = (other.start_row == 0) || (strlen(other.start_row)==0);

  if (start_null || other_start_null) {
    if (start_null != other_start_null)
      return false;
  }
  else {
    if (strcmp(start_row, other.start_row))
      return false;
  }
  if (end_row == 0 || other.end_row == 0) {
    if (end_row != other.end_row)
      return false;
  }
  else {
    if (strcmp(end_row, other.end_row))
      return false;
  }
  return true;
}

bool RangeSpec::operator!=(const RangeSpec &other) const {
  return !(*this == other);
}

bool RangeSpec::operator<(const RangeSpec &other) const {
  bool start_null = (start_row == 0) || (strlen(start_row)==0);
  bool other_start_null = (other.start_row == 0) || (strlen(other.start_row)==0);

  if (start_null || other_start_null) {
    if (!other_start_null)
      return true;
    else if (!start_null)
      return false;
  }
  else {
    int cmpval = strcmp(start_row, other.start_row);
    if (cmpval < 0)
      return true;
    else if (cmpval > 0)
      return false;
  }

  bool end_null = (end_row==0) || (strlen(end_row)==0);
  bool other_end_null = (other.end_row == 0) || (strlen(other.end_row)==0);

  if (end_null || other_end_null) {
    if (!other_end_null)
      return true;
    else if (!end_null)
      return false;
  }
  else {
    int cmpval = strcmp(end_row, other.end_row);
    if (cmpval < 0)
      return true;
    else if (cmpval > 0)
      return false;
  }

  return false;
}

uint8_t RangeSpec::encoding_version() const {
  return 1;
}

size_t RangeSpec::encoded_length_internal() const {
  return encoded_length_vstr(start_row) + encoded_length_vstr(end_row);
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
/// <td>End row</td>
/// </tr>
/// </table>
void RangeSpec::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, start_row);
  Serialization::encode_vstr(bufp, end_row);
}

void RangeSpec::decode_internal(uint8_t version, const uint8_t **bufp,
                                size_t *remainp) {
  HT_TRY("decoding range spec",
    start_row = Serialization::decode_vstr(bufp, remainp);
    end_row = Serialization::decode_vstr(bufp, remainp));
}

void RangeSpecManaged::decode_internal(uint8_t version, const uint8_t **bufp,
                                       size_t *remainp) {
  RangeSpec::decode_internal(version, bufp, remainp);
  *this = *this;
}

/** @relates RangeSpec */
ostream &Hypertable::operator<<(ostream &os, const RangeSpec &range) {
  os <<"{RangeSpec:";
  HT_DUMP_CSTR(os, start, range.start_row);
  HT_DUMP_CSTR(os, end, range.end_row);
  os <<'}';
  return os;
}
