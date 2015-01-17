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
/// Definitions for ReplayStatus request parameters.
/// This file contains definitions for ReplayStatus, a class for encoding and
/// decoding paramters to the <i>replay status</i> %Master operation.

#include <Common/Compat.h>

#include "ReplayStatus.h"

#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::Master::Request::Parameters;

uint8_t ReplayStatus::encoding_version() const {
  return 1;
}

size_t ReplayStatus::encoded_length_internal() const {
  return 12 + Serialization::encoded_length_vstr(m_location);
}

/// @details
/// Encoding is as follows:
/// <table>
/// <tr>
/// <th>Encoding</th>
/// <th>Description</th>
/// </tr>
/// <tr>
/// <td>i64</td>
/// <td>Recovery operation ID</td>
/// </tr>
/// <tr>
/// <td>vstr</td>
/// <td>Proxy name of %RangeServer whose log is being replayed</td>
/// </tr>
/// <tr>
/// <td>i32</td>
/// <td>Recovery plan generation</td>
/// </tr>
/// </table>
void ReplayStatus::encode_internal(uint8_t **bufp) const {
  Serialization::encode_i64(bufp, m_op_id);
  Serialization::encode_vstr(bufp, m_location);
  Serialization::encode_i32(bufp, m_plan_generation);
}

void ReplayStatus::decode_internal(uint8_t version, const uint8_t **bufp,
                                   size_t *remainp) {
  m_op_id = Serialization::decode_i64(bufp, remainp);
  m_location = Serialization::decode_vstr(bufp, remainp);
  m_plan_generation = Serialization::decode_i32(bufp, remainp);
}
