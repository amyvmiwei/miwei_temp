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
/// Definitions for PhantomLoad request parameters.
/// This file contains definitions for PhantomLoad, a class for encoding and
/// decoding paramters to the <i>phantom load</i> %RangeServer function.

#include <Common/Compat.h>

#include "PhantomLoad.h"

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::RangeServer::Request::Parameters;

uint8_t PhantomLoad::encoding_version() const {
  return 1;
}

size_t PhantomLoad::encoded_length_internal() const {
  size_t length = 8 + Serialization::encoded_length_vstr(m_location) +
    (m_fragments.size()*4);
  length += 4;
  for (auto & spec : m_range_specs)
    length += spec.encoded_length();
  length += 4;
  for (auto & state : m_range_states)
    length += state.encoded_length();
  return length;
}

/// @details
/// Encoding is as follows:
/// <table>
/// <tr><th>Encoding</th><th>Description</th></tr>
/// <tr><td>vstr</td><td>Location</td></tr>
/// <tr><td>i32</td><td>Plan generation</td></tr>
/// <tr><td>i32</td><td>Fragment count</td></tr>
/// <tr><td>For each fragment ...</td></tr>
/// <tr><td>i32</td><td>Fragment number</td></tr>
/// <tr><td>i32</td><td>%Range specification count</td></tr>
/// <tr><td>For each range specification ...</td></tr>
/// <tr><td>QualifiedRangeSpec</td><td>Qualified range specification</td></tr>
/// <tr><td>i32</td><td>%Range state count</td></tr>
/// <tr><td>For each range state ...</td></tr>
/// <tr><td>RangeState</td><td>%Range state</td></tr>
/// </table>
void PhantomLoad::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_location);
  Serialization::encode_i32(bufp, m_plan_generation);
  Serialization::encode_i32(bufp, m_fragments.size());
  for (auto fragment : m_fragments)
    Serialization::encode_i32(bufp, fragment);
  Serialization::encode_i32(bufp, m_range_specs.size());
  for (auto & spec : m_range_specs)
    spec.encode(bufp);
  Serialization::encode_i32(bufp, m_range_states.size());
  for (auto & state : m_range_states)
    state.encode(bufp);
}

void PhantomLoad::decode_internal(uint8_t version, const uint8_t **bufp,
                                      size_t *remainp) {
  m_location = Serialization::decode_vstr(bufp, remainp);
  m_plan_generation = Serialization::decode_i32(bufp, remainp);
  size_t count = Serialization::decode_i32(bufp, remainp);
  m_fragments.reserve(count);
  for (size_t i=0; i<count; ++i)
    m_fragments.push_back(Serialization::decode_i32(bufp, remainp));
  count = Serialization::decode_i32(bufp, remainp);
  m_range_specs.reserve(count);
  for (size_t i=0; i<count; ++i) {
    QualifiedRangeSpec spec;
    spec.decode(bufp, remainp);
    m_range_specs.push_back(spec);
  }
  count = Serialization::decode_i32(bufp, remainp);
  m_range_states.reserve(count);
  for (size_t i=0; i<count; ++i) {
    RangeState state;
    state.decode(bufp, remainp);
    m_range_states.push_back(state);
  }
}



