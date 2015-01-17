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
/// Definitions for RegisterServer response parameters.
/// This file contains definitions for RegisterServer, a class for encoding and
/// decoding paramters returned by the <i>register server</i> %Master operation.

#include <Common/Compat.h>

#include "RegisterServer.h"

#include <Hypertable/Lib/Canonicalize.h>

#include <Common/Logger.h>
#include <Common/Serialization.h>
#include <Common/Time.h>

using namespace Hypertable;
using namespace Hypertable::Lib::Master::Response::Parameters;

uint8_t RegisterServer::encoding_version() const {
  return 1;
}

size_t RegisterServer::encoded_length_internal() const {
  size_t length = Serialization::encoded_length_vstr(m_location) + 12;
  for (auto &spec : m_variables)
    length += spec.encoded_length();
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
/// <td>vstr</td>
/// <td>Location (proxy name)</td>
/// </tr>
/// <tr>
/// <td>i64</td>
/// <td>%System state generation</td>
/// </tr>
/// <tr>
/// <td>i32</td>
/// <td>Count of system state variable specifications</td>
/// </tr>
/// <tr>
/// <td>For each system state variable specification ...</td>
/// </tr>
/// <tr>
/// <td>SystemVariable::Spec</td>
/// <td>%System state variable specification</td>
/// </tr>
/// </table>
void RegisterServer::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_location);
  Serialization::encode_i64(bufp, m_generation);
  Serialization::encode_i32(bufp, m_variables.size());
  for (auto &spec : m_variables)
    spec.encode(bufp);
}

void RegisterServer::decode_internal(uint8_t version, const uint8_t **bufp,
			     size_t *remainp) {
  m_location = Serialization::decode_vstr(bufp, remainp);
  m_generation = Serialization::decode_i64(bufp, remainp);
  size_t count = (size_t)Serialization::decode_i32(bufp, remainp);
  m_variables.clear();
  for (size_t i=0; i<count; i++) {
    SystemVariable::Spec spec;
    spec.decode(bufp, remainp);
    m_variables.push_back(spec);
  }
}



