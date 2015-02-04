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
/// Definitions for PhantomCommitRanges request parameters.
/// This file contains definitions for PhantomCommitRanges, a class for encoding
/// and decoding paramters to the <i>phantom commit ranges</i> %RangeServer
/// function.

#include <Common/Compat.h>

#include "PhantomCommitRanges.h"

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::RangeServer::Request::Parameters;

uint8_t PhantomCommitRanges::encoding_version() const {
  return 1;
}

size_t PhantomCommitRanges::encoded_length_internal() const {
  size_t length = 12 + Serialization::encoded_length_vstr(m_location);
  length += 4;
  for (auto & spec : m_range_specs)
    length += spec.encoded_length();
  return length;
}

/// @details
/// Encoding is as follows:
/// <table>
/// <tr><th>Encoding</th><th>Description</th></tr>
/// <tr><td>i64</td><td>%Operation ID</td></tr>
/// <tr><td>vstr</td><td>Location</td></tr>
/// <tr><td>i32</td><td>Plan generation</td></tr>
/// <tr><td>i32</td><td>%Range specification count</td></tr>
/// <tr><td>For each range specification ...</td></tr>
/// <tr><td>QualifiedRangeSpec</td><td>Qualified range specification</td></tr>
/// </table>
void PhantomCommitRanges::encode_internal(uint8_t **bufp) const {
  Serialization::encode_i64(bufp, m_op_id);
  Serialization::encode_vstr(bufp, m_location);
  Serialization::encode_i32(bufp, m_plan_generation);
  Serialization::encode_i32(bufp, m_range_specs.size());
  for (auto & spec : m_range_specs)
    spec.encode(bufp);
}

void PhantomCommitRanges::decode_internal(uint8_t version, const uint8_t **bufp,
                                           size_t *remainp) {
  m_op_id = Serialization::decode_i64(bufp, remainp);
  m_location = Serialization::decode_vstr(bufp, remainp);
  m_plan_generation = Serialization::decode_i32(bufp, remainp);
  size_t count = Serialization::decode_i32(bufp, remainp);
  m_range_specs.reserve(count);
  for (size_t i=0; i<count; ++i) {
    QualifiedRangeSpec spec;
    spec.decode(bufp, remainp);
    m_range_specs.push_back(spec);
  }
}



