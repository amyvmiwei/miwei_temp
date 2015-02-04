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
/// Definitions for AcknowledgeLoad request parameters.
/// This file contains definitions for AcknowledgeLoad, a class for encoding and
/// decoding paramters to the <i>acknowledge load</i> %RangeServer function.

#include <Common/Compat.h>

#include "AcknowledgeLoad.h"

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::RangeServer::Request::Parameters;

AcknowledgeLoad::AcknowledgeLoad(const vector<QualifiedRangeSpec*> &ranges) {
  m_specs.reserve(ranges.size());
  for (auto range_spec : ranges)
    m_specs.push_back(*range_spec);
}


uint8_t AcknowledgeLoad::encoding_version() const {
  return 1;
}

size_t AcknowledgeLoad::encoded_length_internal() const {
  size_t length = 4;
  for (auto &range_spec : m_specs)
    length += range_spec.encoded_length();
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
/// <td>Qualified range specification count</td>
/// </tr>
/// <tr>
/// <td>For each qualified range specification ...</td>
/// </tr>
/// <tr>
/// <td>QualifiedRangeSpec</td>
/// <td>Qualified range specification</td>
/// </tr>
/// </table>
void AcknowledgeLoad::encode_internal(uint8_t **bufp) const {
  Serialization::encode_i32(bufp, m_specs.size());
  for (auto &spec : m_specs)
    spec.encode(bufp);
}

void AcknowledgeLoad::decode_internal(uint8_t version, const uint8_t **bufp,
                                      size_t *remainp) {
  size_t count = (size_t)Serialization::decode_i32(bufp, remainp);
  m_specs.reserve(count);
  for (size_t i=0; i<count; i++) {
    QualifiedRangeSpec spec;
    spec.decode(bufp, remainp);
    m_specs.push_back(spec);
  }
}



