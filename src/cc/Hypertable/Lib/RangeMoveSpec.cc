/** -*- c++ -*-
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
#include "Common/Serialization.h"

#include <cstring>
#include <iostream>

#include "KeySpec.h"
#include "RangeMoveSpec.h"

using namespace std;
using namespace Hypertable;

uint8_t RangeMoveSpec::encoding_version() const {
  return 1;
}

size_t RangeMoveSpec::encoded_length_internal() const {
  return table.encoded_length() + range.encoded_length() +
    Serialization::encoded_length_vstr(source_location) +
    Serialization::encoded_length_vstr(dest_location) + 5;
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
/// <tr>
/// <td>vstr</td>
/// <td>Source location</td>
/// </tr>
/// <tr>
/// <td>vstr</td>
/// <td>Destination location</td>
/// </tr>
/// <tr>
/// <td>i32</td>
/// <td>Error code</td>
/// </tr>
/// <tr>
/// <td>bool</td>
/// <td>Complete flag</td>
/// </tr>
/// </table>
void RangeMoveSpec::encode_internal(uint8_t **bufp) const {
  table.encode(bufp);
  range.encode(bufp);
  Serialization::encode_vstr(bufp, source_location);
  Serialization::encode_vstr(bufp, dest_location);
  Serialization::encode_i32(bufp, error);
  Serialization::encode_bool(bufp, complete);
}

void RangeMoveSpec::decode_internal(uint8_t version, const uint8_t **bufp,
                                      size_t *remainp) {
  table.decode(bufp, remainp);
  range.decode(bufp, remainp);
  source_location = Serialization::decode_vstr(bufp, remainp);
  dest_location = Serialization::decode_vstr(bufp, remainp);
  error = Serialization::decode_i32(bufp, remainp);
  complete = Serialization::decode_bool(bufp, remainp);
}

/** @relates RangeMoveSpec */
ostream &Hypertable::operator<<(ostream &os, const RangeMoveSpec &move_spec) {
  os <<"{RangeMoveSpec: " << move_spec.table << " " << move_spec.range
     <<" source_location="<< move_spec.source_location
     <<" dest_location="<< move_spec.dest_location
     <<" error='" << Error::get_text(move_spec.error)
     << "' complete=" << (move_spec.complete ? "true" : "false") << "}";
  return os;
}
