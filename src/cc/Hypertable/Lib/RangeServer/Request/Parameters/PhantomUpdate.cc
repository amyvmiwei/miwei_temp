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
/// Definitions for PhantomUpdate request parameters.
/// This file contains definitions for PhantomUpdate, a class for encoding and
/// decoding paramters to the <i>phantom update</i> %RangeServer function.

#include <Common/Compat.h>

#include "PhantomUpdate.h"

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::RangeServer::Request::Parameters;

uint8_t PhantomUpdate::encoding_version() const {
  return 1;
}

size_t PhantomUpdate::encoded_length_internal() const {
  return 8 + Serialization::encoded_length_vstr(m_location) +
    m_range_spec.encoded_length();
}

/// @details
/// Encoding is as follows:
/// <table>
/// <tr><th>Encoding</th><th>Description</th></tr>
/// <tr><td>vstr</td><td>Location</td></tr>
/// <tr><td>i32</td><td>Plan generation</td></tr>
/// <tr><td>QualifiedRangeSpec</td><td>Qualified range specification</td></tr>
/// <tr><td>i32</td><td>Fragment ID</td></tr>
/// </table>
void PhantomUpdate::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_location);
  Serialization::encode_i32(bufp, m_plan_generation);
  m_range_spec.encode(bufp);
  Serialization::encode_i32(bufp, m_fragment);
}

void PhantomUpdate::decode_internal(uint8_t version, const uint8_t **bufp,
                                    size_t *remainp) {
  m_location = Serialization::decode_vstr(bufp, remainp);
  m_plan_generation = Serialization::decode_i32(bufp, remainp);
  m_range_spec.decode(bufp, remainp);
  m_fragment = Serialization::decode_i32(bufp, remainp);
}



