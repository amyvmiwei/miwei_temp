/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

/// @file
/// Definitions for AcknowledgeLoad response parameters.
/// This file contains definitions for AcknowledgeLoad, a class for encoding
/// and decoding response paramters from the <i>acknowledge load</i>
/// %RangeServer function.

#include <Common/Compat.h>

#include "AcknowledgeLoad.h"

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::RangeServer::Response::Parameters;

uint8_t AcknowledgeLoad::encoding_version() const {
  return 1;
}

size_t AcknowledgeLoad::encoded_length_internal() const {
  size_t length = 4;
  for (auto &entry : m_error_map)
    length += entry.first.encoded_length() + 4;
  return length;
}

/// @details
/// Encoding is as follows:
/// <table>
/// <tr>
/// <th>Encoding</th>
/// <th>Description</th>
/// </tr>
/// <tr>
/// <td>i32</td>
/// <td>Map entry count</td>
/// </tr>
/// <tr>
/// <td>For each map entry ...</td>
/// </tr>
/// <tr>
/// <td>QualifiedRangeSpec</td>
/// <td>Qualified range specification</td>
/// </tr>
/// <tr>
/// <td>i32</td>
/// <td>Error code</td>
/// </tr>
/// </table>
void AcknowledgeLoad::encode_internal(uint8_t **bufp) const {
  Serialization::encode_i32(bufp, m_error_map.size());
  for (auto &entry : m_error_map) {
    entry.first.encode(bufp);
    Serialization::encode_i32(bufp, entry.second);
  }
}

void AcknowledgeLoad::decode_internal(uint8_t version, const uint8_t **bufp,
                                      size_t *remainp) {
  size_t count = (size_t)Serialization::decode_i32(bufp, remainp);
  for (size_t i=0; i<count; i++) {
    QualifiedRangeSpec spec;
    spec.decode(bufp, remainp);
    int32_t error = Serialization::decode_i32(bufp, remainp);
    m_error_map[spec] = error;
  }
}



