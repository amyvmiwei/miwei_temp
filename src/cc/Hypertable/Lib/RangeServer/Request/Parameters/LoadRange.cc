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
/// Definitions for LoadRange request parameters.
/// This file contains definitions for LoadRange, a class for encoding and
/// decoding paramters to the <i>load range</i> %RangeServer function.

#include <Common/Compat.h>

#include "LoadRange.h"

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::RangeServer::Request::Parameters;

uint8_t LoadRange::encoding_version() const {
  return 1;
}

size_t LoadRange::encoded_length_internal() const {
  return 1 + m_table.encoded_length() + m_range_spec.encoded_length() +
    m_range_state.encoded_length();
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
/// <td>%Table identifier</td>
/// </tr>
/// <tr>
/// <td>RangeSpec</td>
/// <td>%Range specification</td>
/// </tr>
/// <tr>
/// <td>RangeState</td>
/// <td>%Range state</td>
/// </tr>
/// <tr>
/// <td>bool</td>
/// <td><i>needs compaction</i> flag</td>
/// </tr>
/// </table>
void LoadRange::encode_internal(uint8_t **bufp) const {
  m_table.encode(bufp);
  m_range_spec.encode(bufp);
  m_range_state.encode(bufp);
  Serialization::encode_bool(bufp, m_needs_compaction);
}

void LoadRange::decode_internal(uint8_t version, const uint8_t **bufp,
			     size_t *remainp) {
  m_table.decode(bufp, remainp);
  m_range_spec.decode(bufp, remainp);
  m_range_state.decode(bufp, remainp);
  m_needs_compaction = Serialization::decode_bool(bufp, remainp);
}



