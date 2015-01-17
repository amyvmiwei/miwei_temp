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
/// Definitions for RelinquishAcknowledge request parameters.
/// This file contains definitions for RelinquishAcknowledge, a class for encoding and
/// decoding paramters to the <i>relinquish acknowledge</i> %Master operation.

#include <Common/Compat.h>

#include "RelinquishAcknowledge.h"

#include <Hypertable/Lib/Canonicalize.h>

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::Master::Request::Parameters;

uint8_t RelinquishAcknowledge::encoding_version() const {
  return 1;
}

size_t RelinquishAcknowledge::encoded_length_internal() const {
  return Serialization::encoded_length_vstr(m_source) +
    m_table.encoded_length() + m_range_spec.encoded_length();
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
/// <td>TableIdentifier</td>
/// <td>%Table identifier of table to which range belongs</td>
/// </tr>
/// <tr>
/// <td>RangeSpec</td>
/// <td>%Range specification</td>
/// </tr>
/// </table>
void RelinquishAcknowledge::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_source);
  m_table.encode(bufp);
  m_range_spec.encode(bufp);
}

void RelinquishAcknowledge::decode_internal(uint8_t version, const uint8_t **bufp,
                                size_t *remainp) {
  m_source = Serialization::decode_vstr(bufp, remainp);
  m_table.decode(bufp, remainp);
  m_range_spec.decode(bufp, remainp);
}



