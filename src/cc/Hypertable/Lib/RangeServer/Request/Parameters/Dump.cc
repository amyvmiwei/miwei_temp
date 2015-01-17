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
/// Definitions for Dump request parameters.
/// This file contains definitions for Dump, a class for encoding and
/// decoding paramters to the <i>dump</i> %RangeServer function.

#include <Common/Compat.h>

#include "Dump.h"

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::RangeServer::Request::Parameters;

uint8_t Dump::encoding_version() const {
  return 1;
}

size_t Dump::encoded_length_internal() const {
  return 1 + Serialization::encoded_length_vstr(m_fname);
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
/// <td>Output file name</td>
/// </tr>
/// <tr>
/// <td>bool</td>
/// <td>Flag indicating cell cache keys should not be dumped</td>
/// </tr>
/// </table>
void Dump::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_fname);
  Serialization::encode_bool(bufp, m_no_keys);
}

void Dump::decode_internal(uint8_t version, const uint8_t **bufp,
			     size_t *remainp) {
  m_fname = Serialization::decode_vstr(bufp, remainp);
  m_no_keys = Serialization::decode_bool(bufp, remainp);
}



