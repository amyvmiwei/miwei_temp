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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

/// @file
/// Definitions for MoveRange request parameters.
/// This file contains definitions for MoveRange, a class for encoding and
/// decoding paramters to the <i>move range</i> %Master operation.

#include <Common/Compat.h>

#include "MoveRange.h"

#include <Hypertable/Lib/Canonicalize.h>

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::Master::Request::Parameters;

uint8_t MoveRange::encoding_version() const {
  return 1;
}

size_t MoveRange::encoded_length_internal() const {
  return Serialization::encoded_length_vstr(m_source) + 8 +
    m_table.encoded_length() + m_range_spec.encoded_length() +
    Serialization::encoded_length_vstr(m_transfer_log) + 9;
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
/// <td>%RangeServer from which range is being moved</td>
/// </tr>
/// <tr>
/// <td>i64</td>
/// <td>%Range MetaLog entry identifier</td>
/// </tr>
/// <tr>
/// <td>TableIdentifier</td>
/// <td>%Table identifier of table to which range belongs</td>
/// </tr>
/// <tr>
/// <td>RangeSpec</td>
/// <td>%Range specification</td>
/// </tr>
/// <tr>
/// <td>vstr</td>
/// <td>Transfer log</td>
/// </tr>
/// <tr>
/// <td>i64</td>
/// <td>Soft limit</td>
/// </tr>
/// <tr>
/// <td>bool</td>
/// <td>Split flag</td>
/// </tr>
/// </table>
void MoveRange::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_source);
  Serialization::encode_i64(bufp, m_range_id);
  m_table.encode(bufp);
  m_range_spec.encode(bufp);
  Serialization::encode_vstr(bufp, m_transfer_log);
  Serialization::encode_i64(bufp, m_soft_limit);
  Serialization::encode_bool(bufp, m_is_split);
}

void MoveRange::decode_internal(uint8_t version, const uint8_t **bufp,
                                size_t *remainp) {
  m_source = Serialization::decode_vstr(bufp, remainp);
  m_range_id = Serialization::decode_i64(bufp, remainp);
  m_table.decode(bufp, remainp);
  m_range_spec.decode(bufp, remainp);
  m_transfer_log = Serialization::decode_vstr(bufp, remainp);
  m_soft_limit = Serialization::decode_i64(bufp, remainp);
  m_is_split = Serialization::decode_bool(bufp, remainp);
}



