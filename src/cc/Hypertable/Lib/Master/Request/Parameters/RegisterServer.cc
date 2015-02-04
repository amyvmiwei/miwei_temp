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
/// Definitions for RegisterServer request parameters.
/// This file contains definitions for RegisterServer, a class for encoding and
/// decoding paramters to the <i>register server</i> %Master operation.

#include <Common/Compat.h>

#include "RegisterServer.h"

#include <Hypertable/Lib/Canonicalize.h>

#include <Common/Logger.h>
#include <Common/Serialization.h>
#include <Common/Time.h>

using namespace Hypertable;
using namespace Hypertable::Lib::Master::Request::Parameters;

uint8_t RegisterServer::encoding_version() const {
  return 1;
}

size_t RegisterServer::encoded_length_internal() const {
  return Serialization::encoded_length_vstr(m_location) +
    m_system_stats.encoded_length() + 11;
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
/// <td>Location (proxy name)</td>
/// </tr>
/// <tr>
/// <td>i16</td>
/// <td>Listen port</td>
/// </tr>
/// <tr>
/// <td>bool</td>
/// <td>Lock held flag</td>
/// </tr>
/// <tr>
/// <td>StatsSystem</td>
/// <td>%System statistics</td>
/// </tr>
/// <tr>
/// <td>i64</td>
/// <td>Current time (nanoseconds since Epoch)</td>
/// </tr>
/// </table>
void RegisterServer::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_location);
  Serialization::encode_i16(bufp, m_listen_port);
  Serialization::encode_bool(bufp, m_lock_held);
  m_system_stats.encode(bufp);
  Serialization::encode_i64(bufp, get_ts64());
}

void RegisterServer::decode_internal(uint8_t version, const uint8_t **bufp,
			     size_t *remainp) {
  m_location = Serialization::decode_vstr(bufp, remainp);
  m_listen_port = Serialization::decode_i16(bufp, remainp);
  m_lock_held = Serialization::decode_bool(bufp, remainp);
  m_system_stats.decode(bufp, remainp);
  m_now = Serialization::decode_i64(bufp, remainp);
}



